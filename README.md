# US Aviation Data Platform

This project implements a Streamlit-based multi-page application for ingesting and analyzing US aviation datasets.

## Getting Started

1. Create and populate a virtual environment with the dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. Copy `.env.example` to `.env` and update the values as needed. Set `IS_ADMIN=Yes` to access the admin ingest page.
3. Launch the Streamlit application:
   ```bash
   streamlit run app/streamlit_app.py
   ```

## Project Structure

```
app/
  streamlit_app.py          # App entry point and routing
  pages/
    1_Admin_Ingest.py       # Admin-only ingest experience (Step 1)
    2_Simulations.py        # Placeholder for future work (Step 2)
config.toml                # Global configuration
src/
  ingest/                  # Source-specific ingestion helpers
  validation/              # Data quality checks
  utils/                   # Shared utilities (dates, IO, plotting, logging)
data/
  raw/                     # Raw downloaded assets
  interim/                 # Lightly cleaned data
  processed/               # Model-ready Parquet outputs
logs/                      # Ingestion logs
```

## Ingest Workflow

The admin ingest page downloads BTS On-Time Performance flight movements, NOAA METAR weather summaries, and TSA traveler throughput. Each dataset is validated, summarized with Plotly figures, and saved to Parquet along with a manifest and ingest log entry.

## Configuration

Global options live in `config.toml`. Datasets are cached with `st.cache_data` to minimize repeated network calls, and the app runs in dark mode with Plotly's `plotly_dark` template.

## Validation

Custom validation checks ensure schema integrity, date coverage, duplicate handling, and sanity constraints. Results are surfaced in the UI with indicator visualizations and tabular summaries.

## License

MIT
