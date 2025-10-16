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

## How to get keys

### Data.gov (BTS On-Time Performance)
- Sign up for a free account at <https://api.data.gov/signup/>.
- The API key will arrive via email. Copy it into your `.env` file as `DATA_GOV_API_KEY=your_key`.
- Supplying a key raises rate limits and enables authenticated BTS OTP API calls.

### NOAA AWC METAR
- No API key is required, but NOAA expects an identifying email address.
- Set `NOAA_USER_AGENT=youremail@example.com` in your `.env` and the app will include it in request headers.

### TSA Throughput
- The TSA checkpoint throughput CSV is public and requires no credentials.

### OpenSky Network (optional)
- Create a free account at <https://opensky-network.org> to enable trajectory data in future steps.
- Add `OPENSKY_USER` and `OPENSKY_PASS` to your `.env` once credentials are available.

### FlightAware AeroAPI (optional)
- Sign up for FlightAware AeroAPI access and generate a developer key.
- Store the key in `.env` as `FLIGHTAWARE_API_KEY` for later use.

## License

MIT
