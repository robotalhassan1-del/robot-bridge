"""
Robot <-> Gemini Live bridge (test version, runs on your own computer).

Robot (xiaozhi firmware)  <--WebSocket/Opus-->  this server  <--Gemini Live API-->  Google
"""
import asyncio
import json
import os
import socket
import time
import uuid

from aiohttp import web, WSMsgType

import codec

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.environ.get("PORT", "8000"))
MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.8-live")
VOICE = os.environ.get("GEMINI_VOICE", "")           # optional, e.g. "Puck"
SILENCE_MS = int(os.environ.get("SILENCE_MS", "600"))  # wait this long after you stop talking

SYSTEM_PROMPT = (
    "Sunanka Robot. Kai mataimaki ne mai fara'a a cikin wani karamin robot a gidan Alhassan a Nigeria. "
    "Ka fahimci Hausa, kuma KA AMSA DA HAUSA KAWAI sai dai idan mutum ya nemi wani harshe. "
    "Ka yi gajerun amsoshi (jimla daya zuwa uku) domin ana jin su ta lasifika. "
    "Kada ka yi amfani da alamomi ko emoji. Ka yi magana a hankali da bayyananniyar murya."
)


def load_key():
    key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not key:
        path = os.path.join(HERE, "gemini_key.txt")
        if os.path.exists(path):
            key = open(path, encoding="utf-8").read().strip()
    if not key:
        raise SystemExit("Ba a samu API key ba. Ka kirkiri fayil 'gemini_key.txt' a cikin wannan folda ka sanya key a ciki.")
    return key


def lan_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


# ---------------------------------------------------------------- OTA endpoint
async def ota(request: web.Request):
    # behind a proxy (GitHub Codespaces) the real scheme/host come in these headers
    host = request.headers.get("X-Forwarded-Host", request.host)
    secure = request.headers.get("X-Forwarded-Proto", "http").split(",")[0].strip() == "https"
    scheme = "wss" if secure else "ws"
    body = {
        "server_time": {"timestamp": int(time.time() * 1000), "timezone_offset": 60},
        "websocket": {"url": f"{scheme}://{host}/xiaozhi/v1/", "token": ""},
    }
    log("OTA check from", request.remote, "->", body["websocket"]["url"])
    return web.json_response(body)


# ---------------------------------------------------------------- one robot connection
async def robot_ws(request: web.Request):
    ws = web.WebSocketResponse(heartbeat=30, max_msg_size=0)
    await ws.prepare(request)
    device_id = request.headers.get("Device-Id", "?")
    session_id = uuid.uuid4().hex[:8]
    log(f"[{session_id}] robot connected, device {device_id}")

    try:
        pc = codec.make_codec()
    except Exception as e:
        log("OPUS ERROR:", e)
        await ws.close()
        return ws
    log(f"[{session_id}] opus backend: {pc.name}")

    mic_q: asyncio.Queue = asyncio.Queue(maxsize=200)   # PCM 16k chunks robot -> Gemini
    out_q: asyncio.Queue = asyncio.Queue()              # opus frames Gemini -> robot, None = end of turn
    state = {"speaking": False, "alive": True}

    async def send_json(obj):
        if not ws.closed:
            obj["session_id"] = session_id
            await ws.send_str(json.dumps(obj, ensure_ascii=False))

    # ---- task: pace opus frames to the robot
    async def sender():
        sent = 0
        t0 = None
        while state["alive"]:
            item = await out_q.get()
            if item is None:  # end of turn
                if state["speaking"]:
                    await asyncio.sleep(0.3)
                    await send_json({"type": "tts", "state": "stop"})
                    state["speaking"] = False
                sent, t0 = 0, None
                continue
            if not state["speaking"]:
                await send_json({"type": "tts", "state": "start"})
                state["speaking"] = True
                sent, t0 = 0, time.monotonic()
            # keep ~0.3 s ahead of real time, no more
            if sent >= 5:
                due = t0 + (sent - 5) * (codec.FRAME_MS / 1000)
                delay = due - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
            if not ws.closed:
                await ws.send_bytes(item)
            sent += 1

    # ---- task: talk to Gemini (reconnects if the session drops)
    async def gemini_task():
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=load_key())
        speech = None
        if VOICE:
            speech = types.SpeechConfig(voice_config=types.VoiceConfig(
                prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=VOICE)))
        config = types.LiveConnectConfig(
            response_modalities=["AUDIO"],
            system_instruction=SYSTEM_PROMPT,
            speech_config=speech,
            input_audio_transcription=types.AudioTranscriptionConfig(),
            output_audio_transcription=types.AudioTranscriptionConfig(),
            realtime_input_config=types.RealtimeInputConfig(
                automatic_activity_detection=types.AutomaticActivityDetection(
                    silence_duration_ms=SILENCE_MS,
                    prefix_padding_ms=100,
                )
            ),
        )
        while state["alive"]:
            try:
                log(f"[{session_id}] connecting to Gemini ({MODEL}) ...")
                async with client.aio.live.connect(model=MODEL, config=config) as session:
                    log(f"[{session_id}] Gemini connected")

                    async def mic_to_gemini():
                        while True:
                            pcm = await mic_q.get()
                            await session.send_realtime_input(
                                audio=types.Blob(data=pcm, mime_type="audio/pcm;rate=16000"))

                    async def gemini_to_robot():
                        buf = b""
                        said = ""
                        heard = ""
                        t_first = None
                        while True:
                            async for msg in session.receive():
                                sc = msg.server_content
                                if not sc:
                                    continue
                                if sc.interrupted:
                                    while not out_q.empty():
                                        out_q.get_nowait()
                                    buf = b""
                                    await out_q.put(None)
                                if sc.input_transcription and sc.input_transcription.text:
                                    heard += sc.input_transcription.text
                                    await send_json({"type": "stt", "text": heard})
                                if sc.output_transcription and sc.output_transcription.text:
                                    said += sc.output_transcription.text
                                    await send_json({"type": "tts", "state": "sentence_start", "text": said})
                                if sc.model_turn:
                                    for part in sc.model_turn.parts or []:
                                        if part.inline_data and part.inline_data.data:
                                            if t_first is None:
                                                t_first = time.monotonic()
                                            buf += part.inline_data.data
                                            while len(buf) >= codec.SPK_BYTES:
                                                chunk, buf = buf[:codec.SPK_BYTES], buf[codec.SPK_BYTES:]
                                                for pkt in pc.encode(chunk):
                                                    await out_q.put(pkt)
                                if sc.turn_complete:
                                    if buf:
                                        buf += b"\x00" * (codec.SPK_BYTES - len(buf))
                                        for pkt in pc.encode(buf):
                                            await out_q.put(pkt)
                                        buf = b""
                                    await out_q.put(None)
                                    log(f"[{session_id}] turn done. heard='{heard}' said='{said}'")
                                    said = heard = ""
                                    t_first = None

                    t1 = asyncio.create_task(mic_to_gemini())
                    t2 = asyncio.create_task(gemini_to_robot())
                    done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_EXCEPTION)
                    for p in pending:
                        p.cancel()
                    for d in done:
                        d.result()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log(f"[{session_id}] Gemini error: {type(e).__name__}: {e}")
                await asyncio.sleep(2)

    tasks = [asyncio.create_task(sender()), asyncio.create_task(gemini_task())]

    # ---- main loop: messages from the robot
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                data = json.loads(msg.data)
                t = data.get("type")
                if t == "hello":
                    await ws.send_str(json.dumps({
                        "type": "hello", "transport": "websocket", "session_id": session_id,
                        "audio_params": {"format": "opus", "sample_rate": codec.SPK_RATE,
                                         "channels": 1, "frame_duration": codec.FRAME_MS},
                    }))
                    log(f"[{session_id}] hello answered")
                elif t == "listen":
                    log(f"[{session_id}] listen {data.get('state')} {data.get('text', '')}")
                elif t == "abort":
                    while not out_q.empty():
                        out_q.get_nowait()
                    await out_q.put(None)
                    log(f"[{session_id}] abort")
            elif msg.type == WSMsgType.BINARY:
                if state["speaking"]:
                    continue  # half-duplex: ignore mic while the robot talks
                try:
                    pcm = pc.decode(msg.data)
                except Exception as e:
                    log("decode error:", e)
                    continue
                if pcm:
                    if mic_q.full():
                        mic_q.get_nowait()
                    mic_q.put_nowait(pcm)
    finally:
        state["alive"] = False
        for t in tasks:
            t.cancel()
        log(f"[{session_id}] robot disconnected")
    return ws


def main():
    load_key()  # fail early with a clear message
    app = web.Application()
    app.router.add_route("*", "/xiaozhi/ota/", ota)
    app.router.add_get("/xiaozhi/v1/", robot_ws)
    print("=" * 60)
    cs_name = os.environ.get("CODESPACE_NAME")
    if cs_name:
        dom = os.environ.get("GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN", "app.github.dev")
        print(" Uwar garke tana kunne (GitHub Codespaces).")
        print(" OTA URL na robot:  https://%s-%d.%s/xiaozhi/ota/" % (cs_name, PORT, dom))
    else:
        ip = lan_ip()
        print(" Uwar garke tana kunne. Adireshin kwamfutarka (IP):", ip)
        print(" OTA URL na robot:  http://%s:%d/xiaozhi/ota/" % (ip, PORT))
    print("=" * 60, flush=True)
    web.run_app(app, host="0.0.0.0", port=PORT, print=None)


if __name__ == "__main__":
    main()
