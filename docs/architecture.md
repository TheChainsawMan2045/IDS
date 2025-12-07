# System Architecture

The system consists of three core components:

## 1. Data Layer
- Raw data (CSV or PCAP → converted)
- Processed enriched alerts (JSONL)
- Stored in `data/raw` and `data/processed`

## 2. Processing Pipeline
Scripts:
- `make_bulk.py` converts enriched alerts into Elasticsearch bulk format
- LLM enrichment step adds:
  - llm_summary
  - llm_severity (Low/Medium/High/Critical)
  - llm_recommendations

## 3. Elasticsearch + Kibana Stack
- Elasticsearch stores enriched alerts index (`enriched-alerts`)
- Kibana provides visualization and searching

### Diagram
