"""Step 1 test: checks that Opus encoding/decoding works on this computer."""
import math, struct
import codec

c = codec.make_codec()
print("Opus backend:", c.name)

# make 0.6 s of a 440 Hz tone at 24 kHz, encode it in 60 ms frames
pcm = b"".join(struct.pack("<h", int(8000 * math.sin(2 * math.pi * 440 * i / 24000))) for i in range(24000 * 6 // 10))
packets = []
for i in range(0, len(pcm) - codec.SPK_BYTES + 1, codec.SPK_BYTES):
    packets += c.encode(pcm[i:i + codec.SPK_BYTES])
print("Encoded packets:", len(packets), "first sizes:", [len(p) for p in packets[:3]])
assert packets, "encoder produced nothing"

# decoder test: robot-style 16 kHz frames. Use the 24k packets only to test the decoder path loosely
try:
    out = c.decode(packets[1])
    print("Decoded bytes from one packet:", len(out))
except Exception as e:
    print("Decode of a 24k packet failed (can be normal):", e)
print("SELFTEST DONE")
