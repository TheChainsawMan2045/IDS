"""
enrich_alerts.py

Loads CIC-IDS-2017 CSV flow data from data/raw/,
creates alert-like records, and enriches them with
a fake LLM-style summary, severity, and recommendations.

Output:
    data/processed/enriched_alerts.jsonl
"""

from pathlib import Path
import pandas as pd
import json

# --------- SETTINGS ---------

# Change this to match the CSV you download into data/raw/
CSV_FILENAME = "Monday-WorkingHours.pcap_ISCX.csv"

# Use a small number while testing so it runs fast
MAX_ROWS = 100  # set to None to process the whole file


# --------- FAKE LLM ENRICHMENT (NO API NEEDED) ---------

def fake_llm_enrich(flow: dict) -> dict:
    """
    Simulates what an LLM might return for a network alert.
    In a real system, this is where you'd call an LLM API.
    """
    src_ip = flow.get("src_ip", "unknown")
    dst_ip = flow.get("dst_ip", "unknown")
    label = flow.get("label", "Unknown")

    label_lower = str(label).lower()

    # very simple severity heuristic based on label text
    if "ddos" in label_lower or "dos" in label_lower:
        severity = "High"
    elif "bruteforce" in label_lower or "force" in label_lower:
        severity = "Medium"
    elif "benign" in label_lower:
        severity = "Low"
    else:
        severity = "Medium"

    summary = (
        f"Network flow from {src_ip} to {dst_ip} labeled as '{label}'. "
        f"This may represent {label_lower or 'unknown activity'}."
    )

    recommendations = [
        "Review other activity from the source IP.",
        "Check whether the destination host is critical.",
        "If repeated, consider blocking or rate-limiting the source.",
    ]

    return {
        "summary": summary,
        "severity": severity,
        "recommendations": recommendations,
    }


# --------- MAP CSV COLUMNS INTO A STANDARD FLOW DICT ---------

def map_flow_columns(row: pd.Series) -> dict:
    """
    Map common CIC-IDS-2017 column names into a consistent structure.
    """

    def pick(names):
        for name in names:
            if name in row:
                return row[name]
        return None

    flow = {
        "timestamp": pick(["Timestamp", "timestamp", "Time"]),
        "src_ip": pick(["Source IP", "Src IP", "Src IP Addr", "src_ip"]),
        "dst_ip": pick(["Destination IP", "Dst IP", "Dst IP Addr", "dst_ip"]),
        "src_port": pick(["Source Port", "Src Port", "src_port"]),
        "dst_port": pick(["Destination Port", "Dst Port", "dst_port"]),
        "protocol": pick(["Protocol", "protocol"]),
        "label": pick(["Label", "label"]),
    }

    # Clean NaN values
    cleaned = {}
    for k, v in flow.items():
        if pd.isna(v):
            cleaned[k] = None
        else:
            cleaned[k] = v
    return cleaned


# --------- MAIN PIPELINE ---------

def main():
    project_root = Path(__file__).resolve().parents[1]
    data_raw = project_root / "data" / "raw"
    data_processed = project_root / "data" / "processed"

    csv_path = data_raw / CSV_FILENAME
    output_path = data_processed / "enriched_alerts.jsonl"

    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV file not found at {csv_path}. "
            f"Put a CIC-IDS-2017 CSV in data/raw/ and update CSV_FILENAME."
        )

    print(f"[+] Loading data from {csv_path}")
    df = pd.read_csv(csv_path)

    if MAX_ROWS is not None:
        df = df.head(MAX_ROWS)
        print(f"[+] Limiting to first {MAX_ROWS} rows for testing")

    print(f"[+] Loaded {len(df)} rows")

    data_processed.mkdir(parents=True, exist_ok=True)

    num_written = 0
    with output_path.open("w", encoding="utf-8") as f_out:
        for _, row in df.iterrows():
            flow = map_flow_columns(row)
            enrichment = fake_llm_enrich(flow)

            enriched = {
                **flow,
                "llm_summary": enrichment["summary"],
                "llm_severity": enrichment["severity"],
                "llm_recommendations": enrichment["recommendations"],
            }

            f_out.write(json.dumps(enriched) + "\n")
            num_written += 1

    print(f"[+] Wrote {num_written} enriched alerts to {output_path}")
    print("[+] Done. You can index this JSONL file into Elasticsearch later.")


if __name__ == "__main__":
    main()
