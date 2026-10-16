"""Opus <-> PCM helpers. Tries PyAV first (pip only), then opuslib (needs opus.dll)."""

MIC_RATE = 16000      # robot microphone sample rate
SPK_RATE = 24000      # rate we tell the robot to expect
FRAME_MS = 60         # robot frame duration
MIC_SAMPLES = MIC_RATE * FRAME_MS // 1000   # 960
SPK_SAMPLES = SPK_RATE * FRAME_MS // 1000   # 1440
SPK_BYTES = SPK_SAMPLES * 2

BACKEND = None


class _AvCodec:
    name = "pyav"

    def __init__(self):
        import av
        self.av = av
        self.dec = av.CodecContext.create("libopus", "r")
        self.dec.sample_rate = MIC_RATE
        self.dec.layout = "mono"
        self.rs = av.AudioResampler(format="s16", layout="mono", rate=MIC_RATE)
        self.enc = av.CodecContext.create("libopus", "w")
        self.enc.sample_rate = SPK_RATE
        self.enc.layout = "mono"
        self.enc.format = "s16"
        self.enc.bit_rate = 32000
        self.enc.options = {"application": "voip", "frame_duration": str(FRAME_MS)}
        self.enc.open()
        self.pts = 0

    def decode(self, opus_bytes):
        out = b""
        for fr in self.dec.decode(self.av.Packet(opus_bytes)):
            res = self.rs.resample(fr)
            if res is None:
                continue
            if not isinstance(res, (list, tuple)):
                res = [res]
            for r in res:
                out += bytes(r.planes[0])[: r.samples * 2]
        return out

    def encode(self, pcm24k):
        """pcm24k: exactly SPK_BYTES of s16 mono 24 kHz. Returns list of opus packets."""
        fr = self.av.AudioFrame(format="s16", layout="mono", samples=SPK_SAMPLES)
        fr.sample_rate = SPK_RATE
        fr.planes[0].update(pcm24k)
        fr.pts = self.pts
        fr.time_base = self.av.Fraction(1, SPK_RATE)
        self.pts += SPK_SAMPLES
        return [bytes(p) for p in self.enc.encode(fr)]


class _OpuslibCodec:
    name = "opuslib"

    def __init__(self):
        import opuslib
        self.dec = opuslib.Decoder(MIC_RATE, 1)
        self.enc = opuslib.Encoder(SPK_RATE, 1, "voip")

    def decode(self, opus_bytes):
        return self.dec.decode(opus_bytes, MIC_SAMPLES)

    def encode(self, pcm24k):
        return [self.enc.encode(pcm24k, SPK_SAMPLES)]


def make_codec():
    """Return a NEW codec object (one per robot connection)."""
    global BACKEND
    errors = []
    for cls in (_AvCodec, _OpuslibCodec):
        try:
            c = cls()
            BACKEND = c.name
            return c
        except Exception as e:  # noqa
            errors.append(f"{cls.name}: {type(e).__name__}: {e}")
    raise RuntimeError("No Opus backend works. " + " | ".join(errors))
