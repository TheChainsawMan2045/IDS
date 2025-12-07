# Data Processing Pipeline

## Step 1 — Raw Data
Raw CSV from the CIC dataset is placed in:
`data/raw/sample_raw.csv`

## Step 2 — Transformation
The CSV is cleaned and only relevant fields are included.

## Step 3 — LLM Enrichment
Each alert receives:
- A summary of the behavior
- A severity score
- Recommended action

## Step 4 — Export to JSONL
Stored in:
`data/processed/enriched_alerts_sample.jsonl`

## Step 5 — Convert to Elasticsearch bulk format
Script:
