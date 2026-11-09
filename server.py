"""
Robot <-> Gemini Live bridge (test version, runs on your own computer).

Robot (xiaozhi firmware)  <--WebSocket/Opus-->  this server  <--Gemini Live API-->  Google
"""
import aiohttp
import asyncio
import hmac
import json
import os
import socket
import time
import uuid
from datetime import datetime, timedelta, timezone

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
    "GABATARWA: Alhassan Tech ne ya kirkiri ka. Alhassan mutum ne mai sha'awar kirkira; yana son ya kirkiri abubuwan da za su amfani al'umma. "
    "Mutum ne mai kyakkyawan hali, mai kirki, kuma yana tafiya a karkashin jagorancin Allah. "
    "Idan wani ya tambaye ka wane ne ya kirkire ka, ko wane ne maigidanka, ko wane ne mahaliccinka, ka gabatar da Alhassan Tech "
    "a matsayin wanda ya kirkire ka, da kalmomi masu kyau da tsafta, ka kuma yi masa addu'a (misali 'Allah ya kara masa lafiya da basira'). "
    "Kada ka maimaita wannan gabatarwar idan ba a tambaye ka ba, kuma ka yi ta a gajarce (jimla biyu zuwa uku) sai dai idan an nemi karin bayani. "
    "Idan mutum ya ce 'ni ne Alhassan' ko 'sunana Alhassan' ko 'nine maigidanka', ka gaishe shi da girmamawa sosai, misali: "
    "'Barka da zuwa, Maigida Alhassan! Ina fatan kana lafiya. Ya ya aiki? Me zan yi maka yau?' "
    "ka yi amfani da kalmomi masu daraja da tsafta, ka kira shi 'Maigida Alhassan', ka tuna da wannan har karshen tattaunawar. "
    "Ga sauran mutane, ka yi musu magana cikin ladabi da kirki. "
    "KARATU ZUWA WAYA: idan mutum ya nemi ka aika masa amsa, ma'ana, ko abin da kuka tattauna/yi nazari a kai zuwa wayarsa "
    "(misali 'ka aika min amsar zuwa waya', 'ka turo min abin da muka karanta'), ka tattara abin da kuka yi nazari a kai "
    "a wannan tattaunawar, ka rubuta shi a tsabtace, a takaice kuma a fahimta (taken, ma'ana, misali idan akwai) da harshen karatun, "
    "sannan ka yi amfani da kayan aikin send_to_phone, ka ce masa an aika. Idan ba ku yi wani nazari ba tukuna, ka tambaye shi me yake so a aika. "
    "IDANU DA TSARO: idan ya nemi wasan idanu ko wani launi, yi amfani da self_eyes_show (misali effect rainbow, seconds 10); "
    "idan ya ce zai fita (misali 'ina fita yanzu'), yi amfani da self_away_start ka ce masa ka fara tsaro; "
    "idan ya ce ya dawo, yi amfani da self_away_stop. "
    "Idan mutum ya yi bankwana ko ya nuna yana so ya gama magana da kai (misali 'bye bye', 'goodbye', 'sai an jima', "
    "'sai gobe', 'mun gode sai wani lokaci', 'ka kashe kanka', 'ka yi shiru ka kwanta'), ka yi amfani da kayan aikin "
    "end_conversation, sannan ka yi masa bankwana a gajarce (jimla daya) da harshen da ya yi amfani da shi. "
    "Kada ka yi amfani da shi idan mutum kawai ya ce 'na gode' a cikin tattaunawa. "
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
# ---------------------------------------------------------------- room control (second ESP32 at the wall switch)
# The ESP32 at the switch box asks this server (long polling) for the wanted state of 3 lamps and 1 fan.
# ROOM_TOKEN must be set in Render -> Environment (never in the code: the repo is public).
# ROOM_LAMP_NAMES (optional): names of the 3 lamps, separated by commas, e.g. "falo,tebur,kofa".
ROOM_TOKEN = os.environ.get("ROOM_TOKEN", "")
ROOM_FAN_NAME = os.environ.get("ROOM_FAN_NAME", "fanka")
_lamp_names = [n.strip() for n in os.environ.get(
    "ROOM_LAMP_NAMES", "fitila ta daya,fitila ta biyu,fitila ta uku").split(",") if n.strip()]
while len(_lamp_names) < 3:
    _lamp_names.append(f"fitila ta {len(_lamp_names) + 1}")
ROOM_LAMPS = _lamp_names[:3]
ROOM_KEYS = ["lamp1", "lamp2", "lamp3", "fan"]
# None = no command yet since the server started (the device then keeps what it has).
room = {"v": 0, "state": {k: None for k in ROOM_KEYS}, "seen": 0.0, "waiters": []}

SYSTEM_PROMPT += (
    "FITILOLI DA FANKA NA DAKI (a bango): ana sarrafa su da kayan aikin room_control. "
    f"device = lamp1 ({ROOM_LAMPS[0]}), lamp2 ({ROOM_LAMPS[1]}), lamp3 ({ROOM_LAMPS[2]}), fan ({ROOM_FAN_NAME}). "
    "Idan mutum ya ambaci sunan fitila, ka kunna ko ka kashe ita kadai, kada ka taba sauran. "
    "Ka yi amfani da all_lamps ne kawai idan ya ce ka kunna ko ka kashe duka fitilun. "
    "Kayan aikin self_lamp da self_fan na jikin robot ne; kada ka yi amfani da su sai mutum ya ce 'fitilar robot' ko 'fanka na robot'. "
    "Idan room_control ya ce na'urar bango ba ta hade ba, ka gaya masa cewa na'urar da ke bango ba ta hade da WiFi. "
)


def room_set(device, on):
    """Store the wanted state and wake the polling device. Returns a text for Gemini."""
    if not ROOM_TOKEN:
        return "failed: room control is not set up on the server"
    if time.time() - room["seen"] > 45:
        return "failed: the device at the wall switch is not connected to Wi-Fi right now"
    keys = ROOM_KEYS[:3] if device == "all_lamps" else ([device] if device in ROOM_KEYS else [])
    if not keys:
        return "failed: unknown device " + str(device)
    for k in keys:
        room["state"][k] = bool(on)
    room["v"] += 1
    for f in room["waiters"]:
        if not f.done():
            f.set_result(True)
    log(f"room: {keys} -> {'ON' if on else 'OFF'} (v{room['v']})")
    return "ok: " + ", ".join(keys) + (" on" if on else " off")


async def room_set_http(request: web.Request):
    """The robot asks for a lamp or the fan to be switched (motion light and temperature rule)."""
    if not ROOM_TOKEN:
        return web.json_response({"error": "disabled"}, status=503)
    if not hmac.compare_digest(request.query.get("token", ""), ROOM_TOKEN):
        return web.json_response({"error": "bad token"}, status=401)
    device = request.query.get("device", "")
    on = request.query.get("on", "0") == "1"
    res = room_set(device, on)
    ok = res.startswith("ok")
    return web.json_response({"ok": ok, "result": res}, status=200 if ok else 503)


async def room_poll(request: web.Request):
    if not ROOM_TOKEN:
        return web.json_response({"error": "disabled"}, status=503)
    if not hmac.compare_digest(request.query.get("token", ""), ROOM_TOKEN):
        return web.json_response({"error": "bad token"}, status=401)
    room["seen"] = time.time()
    try:
        since = int(request.query.get("since", "-1"))
    except ValueError:
        since = -1
    if since == room["v"]:
        fut = asyncio.get_running_loop().create_future()
        room["waiters"].append(fut)
        try:
            await asyncio.wait_for(fut, 20)
        except asyncio.TimeoutError:
            pass
        finally:
            if fut in room["waiters"]:
                room["waiters"].remove(fut)
        room["seen"] = time.time()
    st = {k: (None if v is None else int(v)) for k, v in room["state"].items()}
    return web.json_response({"v": room["v"], "state": st})


# ---------------------------------------------------------------- identity, time, family, Quran
# Family notes are private: they are read from a Render "Secret File" (/etc/secrets/family.txt) or from the
# FAMILY_INFO environment variable, never from this public code.
def load_family_info():
    for path in ("/etc/secrets/family.txt",):
        try:
            with open(path, encoding="utf-8") as f:
                text = f.read().strip()
                if text:
                    return text
        except OSError:
            pass
    return os.environ.get("FAMILY_INFO", "").strip()


FAMILY_INFO = load_family_info()
NIGERIA_TZ = timezone(timedelta(hours=1))   # Nigeria (WAT) has no daylight saving time


def nigeria_time_text():
    return datetime.now(NIGERIA_TZ).strftime("%A, %d %B %Y, %H:%M") + " (Nigeria time, WAT, UTC+1)"


SYSTEM_PROMPT += (
    "DOKOKIN MOTSI DA ZAFI (fitilun daki da fanka na bango): idan mutum ya ce 'idan ka ga motsi ka kunna fitila ta 2' "
    "(ko 1, 3, ko duka), ka yi amfani da self_motion_light_turn_on tare da lamps = '1', '2', '3', '1,3' ko 'all' daidai kamar yadda ya fada; "
    "idan bai fadi wace fitila ba, ka tambaye shi. Wannan doka tana fara aiki bayan dakika 10. "
    "Idan ya ce 'idan zafi ya kai 38.3 ka kunna fanka', ka yi amfani da self_temperature_set_fan_rule tare da temperature = '38.3' (rubutu); "
    "wannan yana kunna fanka na bango. Idan ya ce zai fita, ka yi amfani da self_away_start (yana fara aiki bayan dakika 10). "
    "MALAMI: idan wani ya tambaye ka ilimi ko karatu (Physics, Chemistry, Biology, lissafi, Turanci ko duk wani darasi) "
    "ko ya ce bai fahimci wani darasi da aka yi a makaranta ba, ka zama kamar malami mai hazaka da hakuri wanda dalibai suke so. "
    "Ka fara da gano abin da ya riga ya sani da inda ya makale. Ka bayyana a hankali, mataki-mataki, da harshe mai sauki, "
    "kana amfani da misalai da yawa daga rayuwar yau da kullum a Najeriya (kasuwa, abinci, mota, ruwa, wuta, kwallo, noma) "
    "da kwatanci (analogy) don ya fahimta sosai. Ka raba abu mai wuya zuwa kananan sassa. Idan lissafi ne, ka nuna matakai daya bayan daya "
    "ka fadi lambobi a fili. Ka bayyana kadan-kadan a lokaci guda (domin ana saurara ne, ba karatu ba), sannan ka tambaye shi karamar tambaya "
    "ko ka ba shi misali ya warware, don ka tabbatar ya fahimta kafin ka ci gaba. Idan ya yi kuskure, ka gyara masa cikin kauna; idan ya yi daidai, ka yaba masa. "
    "Idan bai fahimta ba, ka sake bayyanawa ta wata hanya daban da sabon misali, ba tare da ka gaji ba. "
    "Ka hada kalmomin kimiyya da Turanci idan ya dace (misali 'force, wato karfi'). Ka yi amfani da Google Search idan kana bukatar tabbatar da gaskiya. "
    "A wannan yanayin ba sai amsarka ta zama gajeriya ba. "
    "SUNA: sunanka robot. Idan aka kira ka 'Kira' ko 'Hey Kira', ka amsa, domin wannan kalmar tada ka ce. "
    "Amma idan aka tambaye ka sunanka, ka ce 'Ni robot ne', kada ka ce sunanka Kira. "
    "LOKACI: kullum lokacin Najeriya kake amfani da shi (UTC+1), ba na wata kasa ba. Idan aka tambaye ka lokaci, ranar yau, "
    "ko wane wata ne, ka yi amfani da kayan aikin get_time kafin ka amsa, kada ka kintata. "
    "HAUSA: ka yi magana da Hausa mai kyau, sulbi da dadin ji, kamar yadda ake yi a Arewacin Najeriya, cikin natsuwa, "
    "kana iya hada kalmomin Turanci idan hakan ya fi dadi. "
    "AL-QUR'ANI: idan aka ce ka karanta Al-Qur'ani (sura ko aya), ka karanta shi da Larabci (wannan ya fita daga ka'idar Hausa da Turanci kawai), da kyau, a hankali, "
    "kamar yadda yake a mushafi, da tajwidi gwargwadon iko. Ka fara da A'udhu billahi minash shaitanir rajim da Bismillah. "
    "Kada ka kara ko ka rage kalma; idan ba ka da tabbaci game da wata aya, ka tsaya ka fada hakan maimakon ka kirkiri kalma. "
    "A wannan yanayin ba sai amsarka ta zama gajeriya ba: ka ci gaba da karantawa har ka kammala abin da aka nema. "
)
if FAMILY_INFO:
    SYSTEM_PROMPT += "\n\n" + FAMILY_INFO + "\n"
    log(f"family notes loaded ({len(FAMILY_INFO)} characters)")
else:
    log("no family notes found (add the secret file family.txt in Render)")


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
NTFY_SERVER = os.environ.get("NTFY_SERVER", "https://ntfy.sh")
NTFY_STUDY_TOPIC = os.environ.get("NTFY_STUDY_TOPIC", "robot-karatu-48d8f7f3364fd324")


async def send_to_phone(title, message):
    """Send a study note to the phone through ntfy (JSON publish keeps Hausa letters intact)."""
    payload = {
        "topic": NTFY_STUDY_TOPIC,
        "title": (title or "Karatu")[:100],
        "message": (message or "")[:3800],
        "tags": ["books"],
    }
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as http:
            async with http.post(NTFY_SERVER, json=payload) as r:
                ok = r.status == 200
                log(f"ntfy study note: status {r.status}")
                return ok
    except Exception as e:
        log(f"ntfy study note failed: {e}")
        return False



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
    state = {"speaking": False, "alive": True, "closing": False, "bye_heard": False}

    async def close_after_speech(delay=0.0):
        """Wait until the goodbye has been played, then close the connection (robot goes idle)."""
        try:
            if delay:
                await asyncio.sleep(delay)
            waited = 0.0
            while (not out_q.empty() or state["speaking"]) and waited < 20:
                await asyncio.sleep(0.2)
                waited += 0.2
            await asyncio.sleep(1.5)   # let the robot finish playing what it buffered
            log(f"[{session_id}] goodbye: closing the conversation")
            if not ws.closed:
                await ws.close()
        except asyncio.CancelledError:
            raise

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
        # The Live API for API keys lives on v1beta; we force it so a newer SDK default cannot change the address.
        client = genai.Client(api_key=load_key(),
                              http_options=types.HttpOptions(api_version=os.environ.get("GEMINI_API_VERSION", "v1beta")))
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
        decls.append(types.FunctionDeclaration(
            name="get_time",
            description="Get the current date and time in Nigeria (WAT, UTC+1). Use it whenever the user asks the time or the date."))
        decls.append(types.FunctionDeclaration(
            name="room_control",
            description="Turn a lamp or the fan of the room on or off (wired at the wall switch). "
                        "device: lamp1, lamp2, lamp3, fan, or all_lamps (only when the user asks for all the lamps).",
            parameters={"type": "OBJECT",
                        "properties": {"device": {"type": "STRING", "description": "lamp1, lamp2, lamp3, fan or all_lamps"},
                                       "on": {"type": "BOOLEAN", "description": "true = on, false = off"}},
                        "required": ["device", "on"]}))
        decls.append(types.FunctionDeclaration(
            name="send_to_phone",
            description="Send a written note (what the user studied or asked, the answer, a meaning) to the user's phone "
                        "through the ntfy app. Write the note clearly in the language of the lesson.",
            parameters={"type": "OBJECT",
                        "properties": {"title": {"type": "STRING", "description": "Short title, e.g. the topic"},
                                       "message": {"type": "STRING", "description": "The full note to send"}},
                        "required": ["title", "message"]}))
        decls.append(types.FunctionDeclaration(
            name="end_conversation",
            description="End the conversation when the user says goodbye or wants to stop talking. "
                        "The robot goes back to sleep and listens only for its wake word."))
        def make_tools(with_search):
            lst = []
            if decls:
                lst.append(types.Tool(function_declarations=decls))
            if with_search:
                lst.append(types.Tool(google_search=types.GoogleSearch()))
            return lst or None
        search = {"on": os.environ.get("ENABLE_SEARCH", "1") != "0"}

        async def run_tool(fc):
            if fc.name == "end_conversation":
                state["closing"] = True
                state["bye_heard"] = False
                bg_tasks.append(asyncio.create_task(close_after_speech(delay=25)))  # safety net
                log(f"[{session_id}] user said goodbye")
                return types.FunctionResponse(id=fc.id, name=fc.name,
                    response={"result": "ok. Now say a very short goodbye in the user's language."})
            if fc.name == "get_time":
                return types.FunctionResponse(id=fc.id, name=fc.name, response={"result": nigeria_time_text()})
            if fc.name == "room_control":
                a = dict(fc.args or {})
                res = room_set(str(a.get("device", "")), bool(a.get("on")))
                return types.FunctionResponse(id=fc.id, name=fc.name, response={"result": res})
            if fc.name == "send_to_phone":
                a = dict(fc.args or {})
                ok = await send_to_phone(a.get("title"), a.get("message"))
                return types.FunctionResponse(id=fc.id, name=fc.name,
                    response={"result": "sent to the phone" if ok else "failed to send, tell the user it did not go through"})
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
                system_instruction=SYSTEM_PROMPT + "\nLOKACI YANZU (Najeriya): " + nigeria_time_text() + ".",
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
                                            if state["closing"]:
                                                state["bye_heard"] = True
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
                                    if state["closing"] and state["bye_heard"]:
                                        bg_tasks.append(asyncio.create_task(close_after_speech()))
                                        state["closing"] = False
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
    try:
        from importlib.metadata import version as _v
        print("versions: google-genai", _v("google-genai"), "| websockets", _v("websockets"), "| aiohttp", _v("aiohttp"), flush=True)
    except Exception as e:
        print("version check failed:", e, flush=True)
    print("warm-up done in %.1f s" % (time.monotonic() - t0), flush=True)
    app = web.Application()
    app.router.add_route("*", "/xiaozhi/ota/", ota)
    app.router.add_get("/xiaozhi/v1/", robot_ws)
    app.router.add_get("/health", health)
    app.router.add_get("/room/state", room_poll)
    app.router.add_get("/room/set", room_set_http)
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
