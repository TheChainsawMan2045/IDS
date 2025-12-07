"""
make_bulk.py

Convert enriched alerts in JSONL format into an Elasticsearch
bulk index file.

Input:  data/processed/enriched_alerts.jsonl
Output: data/processed/enriched_alerts_bulk.jsonl
Index:  enriched-alerts
"""

import json
from pathlib import Path


# Paths are relative to the repo root when you run: python scripts/make_bulk.py
INPUT_PATH = Path("data/processed/enriched_alerts.jsonl")
OUTPUT_PATH = Path("data/processed/enriched_alerts_bulk.jsonl")
INDEX_NAME = "enriched-alerts"


def make_bulk(input_path: Path, output_path: Path, index_name: str) -> None:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    print(f"[+] Reading enriched alerts from {input_path}")
    print(f"[+] Writing bulk file to      {output_path}")

    with input_path.open("r", encoding="utf-8") as fin, \
         output_path.open("w", encoding="utf-8") as fout:

        count = 0
        for line in fin:
            line = line.strip()
            if not line:
                continue

            doc = json.loads(line)

            # Bulk header line
            header = {"index": {"_index": index_name}}
            fout.write(json.dumps(header) + "\n")
            # Document line
            fout.write(json.dumps(doc) + "\n")

            count += 1

    print(f"[+] Wrote {count} documents in bulk format.")


if __name__ == "__main__":
    make_bulk(INPUT_PATH, OUTPUT_PATH, INDEX_NAME)
