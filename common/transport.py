import asyncio
import json
import struct
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


class TransportError(RuntimeError):
    pass


@dataclass(frozen=True)
class Frame:
    header: Dict[str, Any]
    payload: bytes = b""


_U32 = struct.Struct("!I")


async def read_frame(reader: asyncio.StreamReader, *, max_header_bytes: int = 1_000_000) -> Frame:
    """
    Frame format:
      u32 header_len
      header_json (utf-8)
      u32 payload_len
      payload_bytes
    """
    raw = await reader.readexactly(4)
    (header_len,) = _U32.unpack(raw)
    if header_len <= 0 or header_len > max_header_bytes:
        raise TransportError(f"invalid header_len={header_len}")

    header_bytes = await reader.readexactly(header_len)
    try:
        header = json.loads(header_bytes.decode("utf-8"))
    except Exception as e:
        raise TransportError(f"invalid header json: {e}") from e

    raw = await reader.readexactly(4)
    (payload_len,) = _U32.unpack(raw)
    if payload_len < 0:
        raise TransportError(f"invalid payload_len={payload_len}")

    payload = b""
    if payload_len:
        payload = await reader.readexactly(payload_len)

    return Frame(header=header, payload=payload)


async def write_frame(writer: asyncio.StreamWriter, frame: Frame) -> None:
    header_bytes = json.dumps(frame.header, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    writer.write(_U32.pack(len(header_bytes)))
    writer.write(header_bytes)
    writer.write(_U32.pack(len(frame.payload)))
    if frame.payload:
        writer.write(frame.payload)
    await writer.drain()


async def request_response(
    host: str,
    port: int,
    req: Frame,
    *,
    timeout_s: float = 10.0,
) -> Frame:
    async def _run() -> Frame:
        reader, writer = await asyncio.open_connection(host, port)
        try:
            await write_frame(writer, req)
            return await read_frame(reader)
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    import contextlib

    return await asyncio.wait_for(_run(), timeout=timeout_s)

