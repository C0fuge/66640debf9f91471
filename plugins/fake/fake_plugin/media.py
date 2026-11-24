"""Deterministic media files, generated in pure Python (no network, no big bundled binaries).

Every generator is a pure function of its arguments, so the same seed always yields the same
bytes. The only bundled binary is ``assets/clip.mp4`` (about 3 KB, 64x64, 2 s), used for
videos and round video notes.
"""

from __future__ import annotations

import hashlib
import json
import math
import struct
import zlib
from pathlib import Path

ASSETS = Path(__file__).resolve().parent / "assets"
CLIP = ASSETS / "clip.mp4"
CLIP_SIZE = (64, 64)
CLIP_SECONDS = 2


def color(key: str) -> tuple[int, int, int]:
    """A stable, reasonably saturated RGB colour for ``key``."""
    h = hashlib.sha256(key.encode()).digest()
    hue = h[0] / 255 * 6
    i, f = int(hue) % 6, hue - int(hue)
    hi, lo = 220, 70
    mid_up, mid_dn = int(lo + (hi - lo) * f), int(hi - (hi - lo) * f)
    return [(hi, mid_up, lo), (mid_dn, hi, lo), (lo, hi, mid_up),
            (lo, mid_dn, hi), (mid_up, lo, hi), (hi, lo, mid_dn)][i]


def png(width: int, height: int, key: str) -> bytes:
    """A gradient picture with diagonal stripes, coloured by ``key``."""
    r0, g0, b0 = color(key)
    r1, g1, b1 = color(key + "/2")
    rows = bytearray()
    for y in range(height):
        t = y / max(1, height - 1)
        base = (int(r0 + (r1 - r0) * t), int(g0 + (g1 - g0) * t), int(b0 + (b1 - b0) * t))
        light = tuple(min(255, c + 40) for c in base)
        rows.append(0)
        for x in range(width):
            rows += bytes(light if ((x + y) // 8) % 2 else base)
    return _png_chunks(width, height, bytes(rows))


def _png_chunks(width: int, height: int, raw: bytes) -> bytes:
    def chunk(tag: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", zlib.crc32(tag + data))

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr) + chunk(b"IDAT", zlib.compress(raw, 9))
            + chunk(b"IEND", b""))


def png_size(data: bytes) -> tuple[int, int] | None:
    if data[:8] == b"\x89PNG\r\n\x1a\n" and len(data) >= 24:
        return struct.unpack(">II", data[16:24])
    return None


def gif(width: int, height: int, key: str, frames: int = 6) -> bytes:
    """An animated GIF: a bar sweeping across a coloured field. 8 colours, uncompressed LZW."""
    a, b = color(key), color(key + "/2")
    palette = [a, b, (255, 255, 255), (0, 0, 0)] + [(0, 0, 0)] * 4
    out = bytearray(b"GIF89a" + struct.pack("<HHBBB", width, height, 0xF2, 0, 0))
    out += b"".join(bytes(c) for c in palette)
    out += b"\x21\xff\x0bNETSCAPE2.0\x03\x01\x00\x00\x00"  # loop forever
    for f in range(frames):
        out += b"\x21\xf9\x04\x00" + struct.pack("<H", 10) + b"\x00\x00"  # 100 ms per frame
        out += b"\x2c" + struct.pack("<HHHHB", 0, 0, width, height, 0)
        bar = (f * width) // frames
        pixels = [
            2 if bar <= x < bar + max(2, width // frames) else (0 if (y // 6) % 2 else 1)
            for y in range(height) for x in range(width)
        ]
        out += b"\x03" + _gif_sub_blocks(_lzw_literal(pixels, 3)) + b"\x00"
    out += b"\x3b"
    return bytes(out)


def _lzw_literal(pixels: list[int], min_code: int) -> bytes:
    """LZW stream of literal codes only, with a clear code every few pixels so the code
    size never grows: valid for every decoder, no compression (fine for tiny GIFs)."""
    clear, eoi, size = 1 << min_code, (1 << min_code) + 1, min_code + 1
    codes: list[int] = []
    for i in range(0, len(pixels), 4):
        codes.append(clear)
        codes.extend(pixels[i:i + 4])
    codes.append(eoi)
    acc = nbits = 0
    out = bytearray()
    for c in codes:
        acc |= c << nbits
        nbits += size
        while nbits >= 8:
            out.append(acc & 0xFF)
            acc >>= 8
            nbits -= 8
    if nbits:
        out.append(acc & 0xFF)
    return bytes(out)


def _gif_sub_blocks(data: bytes) -> bytes:
    return b"".join(bytes([len(data[i:i + 255])]) + data[i:i + 255] for i in range(0, len(data), 255))


def wav(seconds: int, key: str, rate: int = 8000) -> bytes:
    """8-bit mono PCM: a two-tone chirp, pitch chosen by ``key``."""
    f0 = 220 + hashlib.sha256(key.encode()).digest()[0] * 2
    n = max(1, seconds) * rate
    samples = bytes(
        128 + int(90 * math.sin(2 * math.pi * (f0 + 60 * (i // (rate // 4)) % 120) * i / rate)
                  * (0.4 + 0.6 * abs(math.sin(math.pi * i / rate))))
        for i in range(n)
    )
    fmt = struct.pack("<HHIIHH", 1, 1, rate, rate, 1, 8)
    return (b"RIFF" + struct.pack("<I", 36 + n) + b"WAVEfmt " + struct.pack("<I", 16) + fmt
            + b"data" + struct.pack("<I", n) + samples)


def waveform(key: str, bars: int = 48) -> list[int]:
    h = hashlib.sha256(key.encode()).digest() * 2
    return [h[i] % 32 for i in range(bars)]


def lottie(key: str) -> bytes:
    """A minimal Lottie animation (a pulsing, rotating rounded square)."""
    r, g, b = (c / 255 for c in color(key))
    shape = {
        "ty": "gr",
        "it": [
            {"ty": "rc", "p": {"a": 0, "k": [0, 0]}, "s": {"a": 0, "k": [240, 240]}, "r": {"a": 0, "k": 48}},
            {"ty": "fl", "c": {"a": 0, "k": [round(r, 3), round(g, 3), round(b, 3), 1]}, "o": {"a": 0, "k": 100}},
            {"ty": "tr", "p": {"a": 0, "k": [0, 0]}, "a": {"a": 0, "k": [0, 0]}, "s": {"a": 0, "k": [100, 100]},
             "r": {"a": 0, "k": 0}, "o": {"a": 0, "k": 100}},
        ],
    }
    layer = {
        "ddd": 0, "ind": 1, "ty": 4, "nm": "box", "ip": 0, "op": 60, "st": 0, "sr": 1,
        "ks": {
            "o": {"a": 0, "k": 100},
            "r": {"a": 1, "k": [{"t": 0, "s": [0], "i": {"x": [0.5], "y": [1]}, "o": {"x": [0.5], "y": [0]}},
                                {"t": 60, "s": [360]}]},
            "p": {"a": 0, "k": [256, 256, 0]},
            "a": {"a": 0, "k": [0, 0, 0]},
            "s": {"a": 1, "k": [{"t": 0, "s": [80, 80, 100], "i": {"x": [0.5], "y": [1]}, "o": {"x": [0.5], "y": [0]}},
                                {"t": 30, "s": [110, 110, 100], "i": {"x": [0.5], "y": [1]}, "o": {"x": [0.5], "y": [0]}},
                                {"t": 60, "s": [80, 80, 100]}]},
        },
        "shapes": [shape],
    }
    doc = {"v": "5.7.4", "fr": 30, "ip": 0, "op": 60, "w": 512, "h": 512, "nm": key, "ddd": 0,
           "assets": [], "layers": [layer]}
    return json.dumps(doc, separators=(",", ":"), sort_keys=True).encode()


def pdf(title: str) -> bytes:
    """A one-page PDF showing ``title`` (ASCII only)."""
    text = "".join(c for c in title if 32 <= ord(c) < 127 and c not in "()\\") or "Document"
    stream = f"BT /F1 24 Tf 72 720 Td ({text}) Tj ET".encode()
    objs = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R "
        b"/Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for i, o in enumerate(objs, 1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n".encode() + o + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \n".encode()
    out += b"".join(f"{o:010d} 00000 n \n".encode() for o in offsets)
    out += f"trailer\n<< /Size {len(objs) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    return bytes(out)


def text_file(key: str, lines: int = 40) -> bytes:
    return "".join(f"{key} line {i + 1}: the quick brown fox jumps over the lazy dog\n"
                   for i in range(lines)).encode()


def blob(key: str, size: int) -> bytes:
    """``size`` pseudo-random bytes (a sha256 chain): a big file for transfer tests."""
    out = bytearray()
    block = hashlib.sha256(key.encode()).digest()
    while len(out) < size:
        out += block
        block = hashlib.sha256(block).digest()
    return bytes(out[:size])
