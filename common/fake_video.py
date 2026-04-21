import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional


MiB = 1024 * 1024


@dataclass(frozen=True)
class GeneratedFile:
    path: str
    bytes: int


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def generate_file(
    output_path: str,
    *,
    size_bytes: int,
    chunk_bytes: int = 1 * MiB,
) -> GeneratedFile:
    """
    Generate a dummy "video" file with the requested size.

    Notes:
    - This is content-agnostic binary data; for our simulator, only size matters.
    - Writes in chunks to avoid large memory usage.
    """
    if size_bytes < 0:
        raise ValueError("size_bytes must be >= 0")
    if chunk_bytes <= 0:
        raise ValueError("chunk_bytes must be > 0")

    out = Path(output_path)
    _ensure_parent_dir(out)

    remaining = size_bytes
    with out.open("wb") as f:
        while remaining > 0:
            n = min(chunk_bytes, remaining)
            f.write(os.urandom(n))
            remaining -= n

    return GeneratedFile(path=str(out), bytes=size_bytes)


def generate_videos(
    output_dir: str,
    *,
    sizes_mib: Iterable[int],
    name_prefix: str = "video",
) -> List[GeneratedFile]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    generated: List[GeneratedFile] = []
    for s in sizes_mib:
        if s <= 0:
            raise ValueError("sizes_mib must all be > 0")
        p = out_dir / f"{name_prefix}_{s}MiB.bin"
        generated.append(generate_file(str(p), size_bytes=s * MiB))
    return generated


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Generate dummy video files")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_one = sub.add_parser("one", help="Generate a single file")
    p_one.add_argument("--out", required=True)
    p_one.add_argument("--size-mib", type=int, required=True)

    p_batch = sub.add_parser("batch", help="Generate multiple sizes into a folder")
    p_batch.add_argument("--out-dir", required=True)
    p_batch.add_argument("--sizes-mib", required=True, help="Comma-separated, e.g. 1,24,48,72,96,120,144")
    p_batch.add_argument("--prefix", default="video")

    args = parser.parse_args(argv)

    if args.cmd == "one":
        generate_file(args.out, size_bytes=args.size_mib * MiB)
        return 0

    if args.cmd == "batch":
        sizes = [int(x.strip()) for x in args.sizes_mib.split(",") if x.strip()]
        generate_videos(args.out_dir, sizes_mib=sizes, name_prefix=args.prefix)
        return 0

    raise AssertionError("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())

