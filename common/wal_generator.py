import argparse
import json
import random
import os
from pathlib import Path


def generate_wal(out_path: str, size_mib: int) -> None:
    """
    Generate a WAL (Write-Ahead Log) JSON file for demo purposes.

    The WAL contains 500 real INSERT transactions into a sensor_logs table.
    The file is padded to exactly size_mib MiB using a dummy _padding field
    on the last transaction, keeping the JSON fully valid and parseable.
    """
    locations = ["Zone A", "Zone B", "Zone C", "Zone D"]
    target_bytes = size_mib * 1024 * 1024

    # Build transactions (reserve last slot for padded entry)
    transactions = []
    for i in range(1, 500):
        transactions.append(
            {
                "action": "INSERT",
                "table": "sensor_logs",
                "data": {
                    "id": i,
                    "temp": round(random.uniform(20.0, 35.0), 2),
                    "humidity": round(random.uniform(40.0, 90.0), 1),
                    "location": random.choice(locations),
                },
            }
        )

    # Serialize without the last padding entry to calculate needed padding
    partial = json.dumps(transactions, separators=(",", ":"))
    # Account for appending one more element: , {..., "_padding": "<N chars>"}
    # Minimum padding entry overhead (before padding string itself)
    padding_entry_prefix = ',{"action":"INSERT","table":"sensor_logs","data":{"id":500,"temp":25.0,"humidity":60.0,"location":"Zone A"},"_padding":"'
    padding_entry_suffix = '"}'
    closing = "]"

    overhead = len(partial) + len(padding_entry_prefix) + len(padding_entry_suffix) + len(closing)
    # How many padding chars do we need?
    pad_chars = target_bytes - overhead
    if pad_chars < 0:
        # File is already large enough without padding — just use 500 normal entries
        transactions.append(
            {
                "action": "INSERT",
                "table": "sensor_logs",
                "data": {
                    "id": 500,
                    "temp": round(random.uniform(20.0, 35.0), 2),
                    "humidity": round(random.uniform(40.0, 90.0), 1),
                    "location": random.choice(locations),
                },
            }
        )
        json_data = json.dumps(transactions, separators=(",", ":"))
    else:
        # Append a padded entry so the file hits exactly target_bytes
        padding_str = "X" * pad_chars
        padded_entry = {
            "action": "INSERT",
            "table": "sensor_logs",
            "data": {
                "id": 500,
                "temp": 25.0,
                "humidity": 60.0,
                "location": "Zone A",
            },
            "_padding": padding_str,
        }
        transactions.append(padded_entry)
        json_data = json.dumps(transactions, separators=(",", ":"))

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(json_data)

    actual_size = os.path.getsize(out_path)
    print(f"Generated WAL file: {out_path}")
    print(f"  Target: {target_bytes:,} bytes ({size_mib} MiB)")
    print(f"  Actual: {actual_size:,} bytes ({actual_size/1024/1024:.3f} MiB)")
    print(f"  Transactions: {len(transactions)} (incl. padding entry)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a WAL JSON file for EdgeHydra demo")
    parser.add_argument("--out", required=True, help="Output WAL JSON file path")
    parser.add_argument("--size-mib", type=int, required=True, help="Target file size in MiB")
    args = parser.parse_args()

    generate_wal(args.out, args.size_mib)


if __name__ == "__main__":
    main()
