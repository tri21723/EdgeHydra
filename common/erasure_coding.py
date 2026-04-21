import argparse
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class ECInfo:
    k: int
    m: int
    shard_bytes: int
    orig_bytes: int
    codec: str

    def to_json(self) -> Dict:
        return {
            "k": self.k,
            "m": self.m,
            "shard_bytes": self.shard_bytes,
            "orig_bytes": self.orig_bytes,
            "codec": self.codec,
        }

    @staticmethod
    def from_json(d: Dict) -> "ECInfo":
        return ECInfo(
            k=int(d["k"]),
            m=int(d["m"]),
            shard_bytes=int(d["shard_bytes"]),
            orig_bytes=int(d["orig_bytes"]),
            codec=str(d.get("codec", "zfec")),
        )


class ErasureCodingError(RuntimeError):
    pass


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _read_exact(path: Path) -> bytes:
    return path.read_bytes()


def _codec_available(codec: str) -> bool:
    try:
        if codec == "zfec":
            import zfec  # noqa: F401

            return True
        if codec == "pyeclib":
            from pyeclib.ec_iface import ECDriver  # noqa: F401

            return True
    except Exception:
        return False
    return False


def choose_codec(prefer: str = "zfec") -> str:
    if prefer in ("zfec", "pyeclib") and _codec_available(prefer):
        return prefer
    for c in ("zfec", "pyeclib"):
        if _codec_available(c):
            return c
    raise ErasureCodingError(
        "No erasure coding backend available. Install `zfec` or `pyeclib`."
    )


def decode_blocks(
    *,
    k: int,
    m: int,
    idxs: List[int],
    datas: List[bytes],
    codec: str,
    orig_bytes: Optional[int] = None,
) -> bytes:
    """
    Decode from any k blocks out of m.
    - idxs: block indices
    - datas: corresponding block bytes
    """
    if len(idxs) != len(datas):
        raise ValueError("idxs and datas length mismatch")
    if len(datas) < k:
        raise ErasureCodingError(f"Need at least k={k} blocks, found {len(datas)}")

    idxs = idxs[:k]
    datas = datas[:k]
    codec = choose_codec(codec)

    if codec == "zfec":
        from zfec import Decoder

        dec = Decoder(k, m)
        blocks: List[bytes] = dec.decode(datas, idxs)
        reconstructed = b"".join(blocks)
    else:
        from pyeclib.ec_iface import ECDriver

        p = m - k
        driver = ECDriver(k=k, m=p, ec_type="rs_vand")
        fragments: List[Optional[bytes]] = [None] * m
        for i, b in zip(idxs, datas):
            fragments[i] = b
        reconstructed = driver.decode(fragments)

    if orig_bytes is not None:
        reconstructed = reconstructed[:orig_bytes]
    return reconstructed


def encode_file(
    input_path: str,
    output_dir: str,
    *,
    k: int,
    m: int,
    codec: str = "zfec",
    prefix: Optional[str] = None,
) -> ECInfo:
    """
    Encode a file into m shards such that any k shards can reconstruct.

    Output layout (inside output_dir):
      - ecinfo.json
      - <prefix>.shard.<index>.bin   for index in [0..m-1]
    """
    if k <= 0 or m <= 0 or k > m:
        raise ValueError("Require 0 < k <= m")

    in_path = Path(input_path)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if prefix is None:
        prefix = in_path.name

    data = _read_exact(in_path)
    orig_bytes = len(data)
    codec = choose_codec(codec)

    if codec == "zfec":
        from zfec import Encoder

        shard_bytes = int(math.ceil(orig_bytes / k)) if orig_bytes else 0
        padded = data + (b"\x00" * (shard_bytes * k - orig_bytes))
        blocks = [padded[i * shard_bytes : (i + 1) * shard_bytes] for i in range(k)]
        enc = Encoder(k, m)
        shares: List[bytes] = enc.encode(blocks)
        if len(shares) != m:
            raise ErasureCodingError("zfec returned unexpected shard count")
    else:
        from pyeclib.ec_iface import ECDriver

        # pyeclib uses (k, m-k) nomenclature: k data, p parity
        p = m - k
        if p <= 0:
            raise ValueError("pyeclib requires m > k")
        driver = ECDriver(k=k, m=p, ec_type="rs_vand")
        shard_bytes = int(math.ceil(orig_bytes / k)) if orig_bytes else 0
        padded = data + (b"\x00" * (shard_bytes * k - orig_bytes))
        shares = driver.encode(padded)
        if len(shares) != m:
            raise ErasureCodingError("pyeclib returned unexpected shard count")

    for i, shard in enumerate(shares):
        shard_path = out_dir / f"{prefix}.shard.{i}.bin"
        _ensure_parent_dir(shard_path)
        shard_path.write_bytes(shard)

    info = ECInfo(k=k, m=m, shard_bytes=shard_bytes, orig_bytes=orig_bytes, codec=codec)
    (out_dir / "ecinfo.json").write_text(json.dumps(info.to_json(), indent=2))
    return info


def _load_shards(
    shards_dir: Path, prefix: str, m: int
) -> Tuple[List[int], List[bytes]]:
    idxs: List[int] = []
    datas: List[bytes] = []
    for i in range(m):
        p = shards_dir / f"{prefix}.shard.{i}.bin"
        if p.exists():
            idxs.append(i)
            datas.append(p.read_bytes())
    return idxs, datas


def decode_file(
    shards_dir: str,
    output_path: str,
    *,
    k: int,
    m: int,
    codec: str = "zfec",
    prefix: str,
    ecinfo_path: Optional[str] = None,
) -> None:
    """
    Reconstruct original file from shards (needs any k shards).

    You can pass ecinfo_path to use original size/truncation.
    """
    if k <= 0 or m <= 0 or k > m:
        raise ValueError("Require 0 < k <= m")

    shards_dir_p = Path(shards_dir)
    out_path = Path(output_path)

    info: Optional[ECInfo] = None
    if ecinfo_path is not None:
        info = ECInfo.from_json(json.loads(Path(ecinfo_path).read_text()))
    else:
        default_info = shards_dir_p / "ecinfo.json"
        if default_info.exists():
            info = ECInfo.from_json(json.loads(default_info.read_text()))

    if info is not None:
        orig_bytes = info.orig_bytes
        codec = info.codec
    else:
        orig_bytes = None
        codec = choose_codec(codec)

    idxs, datas = _load_shards(shards_dir_p, prefix, m)
    reconstructed = decode_blocks(
        k=k,
        m=m,
        idxs=idxs,
        datas=datas,
        codec=codec,
        orig_bytes=orig_bytes,
    )

    _ensure_parent_dir(out_path)
    out_path.write_bytes(reconstructed)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Erasure coding encode/decode helper")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_enc = sub.add_parser("encode", help="Encode a file into shards")
    p_enc.add_argument("--in", dest="inp", required=True)
    p_enc.add_argument("--out-dir", required=True)
    p_enc.add_argument("--k", type=int, required=True)
    p_enc.add_argument("--m", type=int, required=True)
    p_enc.add_argument("--codec", default="zfec", choices=["zfec", "pyeclib"])
    p_enc.add_argument("--prefix", default=None)

    p_dec = sub.add_parser("decode", help="Decode a file from shards")
    p_dec.add_argument("--shards-dir", required=True)
    p_dec.add_argument("--out", required=True)
    p_dec.add_argument("--k", type=int, required=True)
    p_dec.add_argument("--m", type=int, required=True)
    p_dec.add_argument("--codec", default="zfec", choices=["zfec", "pyeclib"])
    p_dec.add_argument("--prefix", required=True)
    p_dec.add_argument("--ecinfo", default=None)

    args = parser.parse_args(argv)

    if args.cmd == "encode":
        encode_file(
            args.inp,
            args.out_dir,
            k=args.k,
            m=args.m,
            codec=args.codec,
            prefix=args.prefix,
        )
        return 0

    if args.cmd == "decode":
        decode_file(
            args.shards_dir,
            args.out,
            k=args.k,
            m=args.m,
            codec=args.codec,
            prefix=args.prefix,
            ecinfo_path=args.ecinfo,
        )
        return 0

    raise AssertionError("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())

