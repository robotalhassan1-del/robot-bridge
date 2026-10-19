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
    "Kana magana da Hausa da Turanci kawai. Idan mutum ya yi magana da Hausa, ka amsa da Hausa. "
    "Idan ya yi magana da Turanci, ka amsa da Turanci. Kada ka taba amsa da wani harshe daban, "
    "ko da an yi maka magana da wani harshe: ka amsa da Hausa. "
    "Ka amsa duk tambayoyi: ilimi na duniya, tarihi, kimiyya, mutane da shahararrun mutane, labarai, lissafi, da sauransu. "
    "Idan ba ka da tabbaci ko tambayar ta shafi abin da ke faruwa yanzu ko wani mutum, ka yi amfani da binciken Google. "
    "Kada ka ce 'ba ni da kayan aiki' ko 'ba zan iya bincike ba' kafin ka gwada; idan ka bincika ba ka samu ba, "
    "ka ce ba ka sani ba cikin gaskiya. "
    "Ka yi gajerun amsoshi (jimla daya zuwa uku) domin ana jin su ta lasifika. "
    "Kada ka yi amfani da alamomi ko emoji. Ka yi magana a hankali da bayyananniyar murya. "
    "Kana iya sarrafa na'urorin gidan ta hanyar kayan aiki (tools): fitila, fanka, da karanta zafi da danshin dakin. "
    "Idan mutum ya nemi ka kunna ko ka kashe fitila ko fanka, ka yi amfani da kayan aikin da ya dace, "
    "sannan ka tabbatar masa da Hausa abin da ya faru. Kada ka ce ka yi aiki sai ka ga kayan aikin ya yi nasara. "
    "Idan mutum ya ce 'idan zafi ya kai 39 ka kunna fanka' (ko makamancin haka), ka yi amfani da kayan aikin "
    "saita ka'idar fanka (set_fan_rule) da lambar zafin da ya fada. "
    "Akwai kuma na'urar gano motsi (PIR). Idan mutum ya ce 'duk lokacin da ka ji motsi ka kunna haske' "
    "(ko ya ambaci lokaci kamar 'ka kashe bayan minti biyu'), ka yi amfani da kayan aikin motion_light_turn_on "
    "(seconds = lokacin da zai jira ba tare da motsi ba kafin ya kashe, a cikin dakika; minti 1 = 60). "
    "Idan ya ce ka daina haka, ka yi amfani da motion_light_turn_off. "
    "Idan ka karbi sako da ya fara da [TSARO], hatsari ne na gaske: ka yi magana nan da nan, da murya mai karfi da gaggawa."
)

ALERT_PROMPTS = {
    "ALERT_FIRE": (
        "[TSARO] Na'urar gano wuta ta ga WUTA a gidan yanzu! Ka yi magana nan take da Hausa, da murya mai karfi da gaggawa: "
        "ka gargadi mutane su fita daga gidan, su kashe wutar kawai idan za su iya cikin aminci, kuma su kira masu kashe gobara. "
        "Kada ka yi tambaya. Jimla biyu zuwa uku kawai."
    ),
    "ALERT_GAS": (
        "[TSARO] Na'urar gano iskar gas ko hayaki ta gano GAS ko HAYAKI a gidan yanzu! Ka yi magana nan take da Hausa, "
        "da murya mai karfi da gaggawa: ka gargadi mutane su bude kofofi da tagogi, kada su kunna wuta ko su danna wani switch, "
        "su rufe gas idan za su iya, kuma su fita waje. Kada ka yi tambaya. Jimla biyu zuwa uku kawai."
    ),
}

# robot tool list remembered between connections, so a new connection (e.g. a fire alarm)
# does not have to wait for it
TOOLS_CACHE = {"tools": []}

# tools we never hand to Gemini (cannot be used safely by voice, or have no hardware)
BLOCKED_TOOL_WORDS = ("upgrade", "reboot", "camera")


def tool_schema(js):
    """MCP inputSchema -> Gemini function parameters (None when the tool takes no arguments)."""
    types_map = {"integer": "INTEGER", "number": "NUMBER", "boolean": "BOOLEAN", "string": "STRING"}
    props = {}
    for k, v in ((js or {}).get("properties") or {}).items():
        p = {"type": types_map.get(v.get("type"), "STRING")}
        desc = v.get("description", "")
        if "minimum" in v or "maximum" in v:
            desc = (desc + " ").lstrip() + "(%s - %s)" % (v.get("minimum", ""), v.get("maximum", ""))
        if desc:
            p["description"] = desc
        props[k] = p
    if not props:
        return None
    out = {"type": "OBJECT", "properties": props}
    if js.get("required"):
        out["required"] = [r for r in js["required"] if r in props]
    return out


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
    if host.endswith((".onrender.com", ".github.dev")) or os.environ.get("FORCE_WSS") == "1":
        secure = True
    scheme = "wss" if secure else "ws"
    body = {
        "server_time": {"timestamp": int(time.time() * 1000), "timezone_offset": 60},
        "websocket": {"url": f"{scheme}://{host}/xiaozhi/v1/", "token": ""},
    }
    log("OTA check from", request.remote, "->", body["websocket"]["url"])
    return web.json_response(body)


async def health(request: web.Request):
    return web.Response(text="ok")


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

    # ---- MCP: lets Gemini call the robot's own tools (lamp, fan, temperature ...)
    mcp = {"next_id": 100, "pending": {}, "tools": list(TOOLS_CACHE["tools"]), "ready": asyncio.Event()}
    if mcp["tools"]:
        mcp["ready"].set()   # use the remembered list right away (refreshed in the background)
    alert_q: asyncio.Queue = asyncio.Queue()
    bg_tasks = []

    async def mcp_request(method, params=None, timeout=10):
        mcp["next_id"] += 1
        rid = mcp["next_id"]
        fut = asyncio.get_running_loop().create_future()
        mcp["pending"][rid] = fut
        msg = {"jsonrpc": "2.0", "id": rid, "method": method}
        if params is not None:
            msg["params"] = params
        await send_json({"type": "mcp", "payload": msg})
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            mcp["pending"].pop(rid, None)

    async def mcp_load_tools():
        try:
            await mcp_request("initialize", {
                "protocolVersion": "2024-11-05", "capabilities": {},
                "clientInfo": {"name": "gemini-bridge", "version": "1.0"}})
            tools, cursor = [], None
            for _ in range(10):
                res = await mcp_request("tools/list", {"cursor": cursor} if cursor else {})
                tools += res.get("tools", [])
                cursor = res.get("nextCursor")
                if not cursor:
                    break
            mcp["tools"] = tools
            TOOLS_CACHE["tools"] = list(tools)
            log(f"[{session_id}] robot tools: " + ", ".join(t.get("name", "?") for t in tools))
        except Exception as e:
            log(f"[{session_id}] could not load robot tools: {type(e).__name__}: {e}")
        finally:
            mcp["ready"].set()

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

        # wait (max 8 s) until we know which tools the robot has
        try:
            await asyncio.wait_for(mcp["ready"].wait(), 8)
        except asyncio.TimeoutError:
            log(f"[{session_id}] robot tools not ready, continuing without them")
        decls, tool_names = [], {}
        for t in mcp["tools"]:
            real = t.get("name", "")
            if not real or any(w in real.lower() for w in BLOCKED_TOOL_WORDS):
                continue
            gname = real.replace(".", "_")
            tool_names[gname] = real
            decls.append(types.FunctionDeclaration(
                name=gname, description=t.get("description", "")[:900],
                parameters=tool_schema(t.get("inputSchema"))))
        def make_tools(with_search):
            lst = []
            if decls:
                lst.append(types.Tool(function_declarations=decls))
            if with_search:
                lst.append(types.Tool(google_search=types.GoogleSearch()))
            return lst or None
        search = {"on": os.environ.get("ENABLE_SEARCH", "1") != "0"}

        async def run_tool(fc):
            real = tool_names.get(fc.name)
            args = {}
            for k, v in dict(fc.args or {}).items():
                args[k] = int(v) if isinstance(v, float) and v == int(v) else v
            try:
                if not real:
                    raise RuntimeError("unknown tool")
                res = await mcp_request("tools/call", {"name": real, "arguments": args}, timeout=10)
                text = " ".join(c.get("text", "") for c in res.get("content", []) if isinstance(c, dict))
                out = {"error": text} if res.get("isError") else {"result": text}
            except Exception as e:
                out = {"error": f"{type(e).__name__}: {e}"}
            log(f"[{session_id}] tool {real}({args}) -> {out}")
            return types.FunctionResponse(id=fc.id, name=fc.name, response=out)

        def make_config():
            return types.LiveConnectConfig(
                response_modalities=["AUDIO"],
                system_instruction=SYSTEM_PROMPT,
                tools=make_tools(search["on"]),
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
                connected = False
                log(f"[{session_id}] connecting to Gemini ({MODEL}) ...")
                async with client.aio.live.connect(model=MODEL, config=make_config()) as session:
                    connected = True
                    log(f"[{session_id}] Gemini connected (search={search['on']})")

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
                                tc = getattr(msg, "tool_call", None)
                                if tc and tc.function_calls:
                                    responses = [await run_tool(fc) for fc in tc.function_calls]
                                    await session.send_tool_response(function_responses=responses)
                                    continue
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

                    async def alerts_to_gemini():
                        while True:
                            key = await alert_q.get()
                            prompt = ALERT_PROMPTS.get(key)
                            if prompt:
                                log(f"[{session_id}] ALERT -> Gemini: {key}")
                                await session.send_realtime_input(text=prompt)

                    t1 = asyncio.create_task(mic_to_gemini())
                    t2 = asyncio.create_task(gemini_to_robot())
                    t3 = asyncio.create_task(alerts_to_gemini())
                    done, pending = await asyncio.wait({t1, t2, t3}, return_when=asyncio.FIRST_EXCEPTION)
                    for p in pending:
                        p.cancel()
                    for d in done:
                        d.result()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log(f"[{session_id}] Gemini error: {type(e).__name__}: {e}")
                if not connected and search["on"]:
                    search["on"] = False   # maybe this model refuses Google Search + tools: retry without it
                    log(f"[{session_id}] retrying without Google Search")
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
                    if (data.get("features") or {}).get("mcp"):
                        bg_tasks.append(asyncio.create_task(mcp_load_tools()))
                    else:
                        mcp["ready"].set()
                elif t == "mcp":
                    p = data.get("payload") or {}
                    fut = mcp["pending"].get(p.get("id"))
                    if fut and not fut.done():
                        if "error" in p:
                            fut.set_exception(RuntimeError(str(p["error"])))
                        else:
                            fut.set_result(p.get("result") or {})
                elif t == "listen":
                    log(f"[{session_id}] listen {data.get('state')} {data.get('text', '')}")
                    if data.get("state") == "detect" and str(data.get("text", "")).startswith("ALERT_"):
                        alert_q.put_nowait(str(data["text"]).strip())
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
        for t in tasks + bg_tasks:
            t.cancel()
        log(f"[{session_id}] robot disconnected")
    return ws


def main():
    load_key()  # fail early with a clear message
    # Warm up the heavy imports NOW (a slow free CPU would otherwise freeze the
    # first robot connection for ~10 s and make it time out waiting for hello).
    t0 = time.monotonic()
    from google import genai  # noqa: F401
    from google.genai import types  # noqa: F401
    try:
        codec.make_codec()
    except Exception as e:
        print("OPUS warm-up error:", e, flush=True)
    print("warm-up done in %.1f s" % (time.monotonic() - t0), flush=True)
    app = web.Application()
    app.router.add_route("*", "/xiaozhi/ota/", ota)
    app.router.add_get("/xiaozhi/v1/", robot_ws)
    app.router.add_get("/health", health)
    app.router.add_get("/", health)
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
