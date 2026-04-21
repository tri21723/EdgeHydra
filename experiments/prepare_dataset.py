import argparse
from pathlib import Path
from typing import List, Optional

from common.erasure_coding import encode_file
from common.fake_video import generate_videos


def _parse_sizes(s: str) -> List[int]:
    out: List[int] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    if not out:
        raise ValueError("sizes list is empty")
    return out


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prepare experiment dataset: generate raw files and optionally EC-encode shards."
    )
    parser.add_argument(
        "--raw-dir",
        default="experiments/data/raw",
        help="Where raw dummy files are stored (default: experiments/data/raw)",
    )
    parser.add_argument(
        "--shards-root",
        default="experiments/data/shards",
        help="Where per-file shard folders are stored (default: experiments/data/shards)",
    )
    parser.add_argument(
        "--sizes-mib",
        default="1,24,48,72,96,120,144",
        help="Comma-separated MiB sizes (default matches paper range checkpoints)",
    )
    parser.add_argument("--prefix", default="video", help="Filename prefix (default: video)")
    parser.add_argument(
        "--encode",
        action="store_true",
        help="If set, also erasure-encode each raw file into shards",
    )
    parser.add_argument("--k", type=int, default=29, help="EC k (default: 29)")
    parser.add_argument("--m", type=int, default=32, help="EC m (default: 32, i.e., n=32)")
    parser.add_argument(
        "--codec",
        default="zfec",
        choices=["zfec", "pyeclib"],
        help="EC backend (default: zfec)",
    )

    args = parser.parse_args(argv)

    raw_dir = Path(args.raw_dir)
    shards_root = Path(args.shards_root)
    sizes = _parse_sizes(args.sizes_mib)

    raw_dir.mkdir(parents=True, exist_ok=True)
    shards_root.mkdir(parents=True, exist_ok=True)

    generated = generate_videos(str(raw_dir), sizes_mib=sizes, name_prefix=args.prefix)

    if args.encode:
        for g in generated:
            inp = Path(g.path)
            out_dir = shards_root / inp.stem
            encode_file(
                str(inp),
                str(out_dir),
                k=args.k,
                m=args.m,
                codec=args.codec,
                prefix=inp.name,
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

