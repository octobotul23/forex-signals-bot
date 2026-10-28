"""Draws launcher/forexbot.ico (16-256 px) and launcher/forexbot-logo.png (44 px): three gold candles rising on the
ONE ink tile. Pure Python, no Pillow.

    .venv\\Scripts\\python launcher\\make_icon.py
"""
import struct
import zlib
from pathlib import Path

OUT = Path(__file__).resolve().parent / "forexbot.ico"
LOGO = Path(__file__).resolve().parent / "forexbot-logo.png"
INK, GOLD, PAPER = (16, 16, 16), (201, 169, 110), (241, 239, 233)
SIZE = 256
SUB = 4                                   # 4x4 samples per pixel: smooth edges


def _rounded(x, y, x0, y0, x1, y1, r):
    if not (x0 <= x <= x1 and y0 <= y <= y1):
        return False
    cx = min(max(x, x0 + r), x1 - r)
    cy = min(max(y, y0 + r), y1 - r)
    return (x - cx) ** 2 + (y - cy) ** 2 <= r * r


# (shape test, rgba) from the bottom up: the tile, then each candle's wick and body
CANDLES = [(76, 128, 188, 108, 206, 0.55), (128, 92, 156, 70, 176, 0.8), (180, 56, 122, 36, 142, 1.0)]
SHAPES = [(lambda x, y: _rounded(x, y, 8, 8, 248, 248, 52), INK + (1.0,))]
for cx, top, bottom, wick_top, wick_bottom, a in CANDLES:
    SHAPES.append((lambda x, y, cx=cx, t=wick_top, b=wick_bottom: _rounded(x, y, cx - 4, t, cx + 4, b, 4), GOLD + (a,)))
    SHAPES.append((lambda x, y, cx=cx, t=top, b=bottom: _rounded(x, y, cx - 17, t, cx + 17, b, 6), GOLD + (a,)))
SHAPES.append((lambda x, y: _rounded(x, y, 8, 8, 248, 248, 52) and not _rounded(x, y, 11, 11, 245, 245, 49),
               PAPER + (0.10,)))           # hairline edge, like the app's glass cards


def draw() -> list[list[tuple[float, float, float, float]]]:
    """Premultiplied RGBA floats, SIZE x SIZE."""
    px = []
    step = 1 / SUB
    for py in range(SIZE):
        row = []
        for qx in range(SIZE):
            acc = [0.0, 0.0, 0.0, 0.0]
            for sy in range(SUB):
                for sx in range(SUB):
                    x, y = qx + (sx + 0.5) * step, py + (sy + 0.5) * step
                    c = [0.0, 0.0, 0.0, 0.0]
                    for inside, (r, g, b, a) in SHAPES:
                        if inside(x, y):            # "over": paint on top of what is below
                            c = [r * a + c[0] * (1 - a), g * a + c[1] * (1 - a), b * a + c[2] * (1 - a),
                                 a + c[3] * (1 - a)]
                    for i in range(4):
                        acc[i] += c[i]
            n = SUB * SUB
            row.append(tuple(v / n for v in acc))
        px.append(row)
    return px


def shrink(px, size):
    """Box filter: each small pixel is the mean of the big pixels whose centres fall inside it."""
    f = SIZE / size

    def span(i):
        return range(int(i * f + 0.5), max(int(i * f + 0.5) + 1, int((i + 1) * f + 0.5)))
    out = []
    for y in range(size):
        row = []
        for x in range(size):
            acc, n = [0.0] * 4, 0
            for sy in span(y):
                for sx in span(x):
                    n += 1
                    for i, v in enumerate(px[sy][sx]):
                        acc[i] += v
            row.append(tuple(v / n for v in acc))
        out.append(row)
    return out


def _rgba8(c):
    r, g, b, a = c
    if a <= 0:
        return 0, 0, 0, 0
    return (round(min(255, r / a)), round(min(255, g / a)), round(min(255, b / a)), round(a * 255))


def png(px) -> bytes:
    h, w = len(px), len(px[0])
    raw = b"".join(b"\x00" + bytes(v for c in row for v in _rgba8(c)) for row in px)

    def chunk(kind, data):
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 6, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b""))


def bmp(px) -> bytes:
    """32-bit icon bitmap (BGRA rows bottom-up, then an all-clear AND mask): what older icon readers expect."""
    h, w = len(px), len(px[0])
    head = struct.pack("<IiiHHIIiiII", 40, w, h * 2, 1, 32, 0, 0, 0, 0, 0, 0)
    rows = b""
    for row in reversed(px):
        for c in row:
            r, g, b, a = _rgba8(c)
            rows += bytes((b, g, r, a))
    mask = b"\x00" * (((w + 31) // 32) * 4 * h)
    return head + rows + mask


def main():
    big = draw()
    images = [(s, bmp(shrink(big, s))) for s in (16, 24, 32, 48, 64)] + [(256, png(big))]
    offset = 6 + 16 * len(images)
    head, body = struct.pack("<HHH", 0, 1, len(images)), b""
    for s, data in images:
        head += struct.pack("<BBBBHHII", s % 256, s % 256, 0, 0, 1, 32, len(data), offset + len(body))
        body += data
    OUT.write_bytes(head + body)
    LOGO.write_bytes(png(shrink(big, 44)))          # the window's header (Tk reads PNG, not ICO)
    print(f"wrote {OUT} ({OUT.stat().st_size} bytes) and {LOGO.name}")


if __name__ == "__main__":
    main()
