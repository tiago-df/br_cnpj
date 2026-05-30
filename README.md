# BR CNPJ → POI Pipeline

Builds a Points of Interest database from Brazil's public CNPJ data (Receita Federal).

## Objective

Extract active, non-MEI businesses from the monthly Receita Federal dump and produce
clean, analysis-ready datasets (Parquet, CSV, JSON Lines, GeoJSON) for downstream
geocoding and POI enrichment workflows.

---

## Directory Structure

```
br_cnpj/
├── app/
│   ├── main.py               # Orchestrator — entry point
│   ├── download.py           # Step 0: download dump zips
│   ├── step_01_filter.py     # Step 1: filter raw zips → intermediate Parquet
│   ├── step_02_join.py       # Step 2: join + enrich → joined Parquet
│   ├── step_03_export.py     # Step 3: export final formats
│   ├── schema.py             # Column definitions (single source of truth)
│   ├── config_loader.py      # Loads config.yaml
│   └── utils/
│       └── logging_utils.py  # File + console logging
│
├── data/
│   ├── input/
│   │   └── 2026-05/          # Versioned download directory (YYYY-MM)
│   ├── intermediate/         # Step 1 and Step 2 Parquet checkpoints
│   ├── cache/                # Reserved for geocoding cache
│   └── output/
│       └── 2026-05/          # Final output files
│
├── logs/                     # Rotating log files (pipeline_YYYY-MM-DD.log)
├── config.yaml               # Non-sensitive configuration
├── .env                      # Secrets (API keys) — not committed
├── .env.example              # Template for .env
├── requirements.txt
├── install.sh                # Linux installation script
└── install.ps1               # Windows installation script
```

---

## Installation

### Linux

```bash
bash install.sh
# or to a custom directory:
bash install.sh --dir /opt/cnpj-poi
```

### Windows

```powershell
.\install.ps1
# or to a custom directory:
.\install.ps1 -InstallDir "C:\tools\cnpj-poi"
```

---

## Configuration

All non-sensitive settings live in [`config.yaml`](config.yaml):

| Key | Description |
|-----|-------------|
| `source.base_url` | Receita Federal dump index URL |
| `filters.situacao_ativa` | Active registration code (`02`) |
| `filters.excluir_porte_mei` | MEI size code to exclude (`05`) |
| `filters.excluir_natureza_mei` | MEI legal nature to exclude (`2135`) |
| `duckdb.memory_limit` | DuckDB memory cap (default `24GB`) |
| `download.workers` | Parallel download threads |

Secrets (future geocoding API keys) go in `.env` — see `.env.example`.

---

## Usage

Activate the virtual environment first:

```bash
source venv/bin/activate          # Linux/macOS
.\venv\Scripts\Activate.ps1       # Windows
```

### Full pipeline run

```bash
python -m app.main --dump-date 2026-05
```

### Resume from a specific step

```bash
# Skip download (files already in data/input/2026-05/)
python -m app.main --dump-date 2026-05 --from-step filter
# or equivalently:
python -m app.main --dump-date 2026-05 --skip-download

# Skip download + filter (intermediates already exist)
python -m app.main --dump-date 2026-05 --from-step join

# Re-export only
python -m app.main --dump-date 2026-05 --from-step export
```

### Force reprocessing

```bash
# Reprocess all steps (overwrite existing intermediates and outputs)
python -m app.main --dump-date 2026-05 --force
```

### Output formats

```bash
python -m app.main --dump-date 2026-05 --format parquet csv jsonl geojson
```

### Geographic and CNAE filters

```bash
# São Paulo and Rio de Janeiro only
python -m app.main --dump-date 2026-05 --uf SP RJ

# Specific CNAE codes
python -m app.main --dump-date 2026-05 --cnae 4711301 4711302

# Combined
python -m app.main --dump-date 2026-05 --uf SP --cnae 4711301
```

### Quick row count (no export)

```bash
python -m app.main --dump-date 2026-05 --from-step export --count
```

### Download only

```bash
python -m app.download --dump-date 2026-05
python -m app.download --dump-date 2026-05 --types emp estab cnae natureza municipio
python -m app.download --dump-date 2026-05 --resume   # resume partial downloads
python -m app.download --dump-date 2026-05 --dry-run  # list files only
```

---

## Filtered Export (`export_filter`)

After running the pipeline, export a filtered subset to any directory without re-running the full pipeline.

### By OSM category and state

```bash
# Gas stations in Roraima → /mnt/disk1/export/amenity_fuel_RR.parquet
python -m app.export_filter --uf RR --category amenity=fuel /mnt/disk1/export/

# Supermarkets in São Paulo and Rio de Janeiro
python -m app.export_filter --uf SP RJ --category shop=supermarket /mnt/disk1/sp_rj/

# All amenity=* (restaurants, banks, pharmacies, hospitals…) in Amazonas
python -m app.export_filter --uf AM --category amenity /mnt/disk1/am_amenities/

# Hotels nationwide, CSV format
python -m app.export_filter --category tourism=hotel --format csv /mnt/disk1/hotels/

# Multiple output formats at once
python -m app.export_filter --uf RR --category amenity=fuel --format parquet csv /mnt/disk1/export/
```

### By CNAE code directly

```bash
# By raw CNAE code (7 digits, no formatting)
python -m app.export_filter --cnae 4731800 --uf SP /mnt/disk1/fuel_sp/

# Multiple CNAE codes
python -m app.export_filter --cnae 4711301 4711302 4712100 /mnt/disk1/markets/
```

### Count without exporting

```bash
# How many active gas stations in Brazil?
python -m app.export_filter --category amenity=fuel --count

# How many POIs in Roraima?
python -m app.export_filter --uf RR --count
```

### Output filename convention

The output file is named automatically from the applied filters:

| Filters | Output filename |
|---------|----------------|
| `--category amenity=fuel --uf RR` | `amenity_fuel_RR.parquet` |
| `--category shop=supermarket --uf SP RJ` | `shop_supermarket_RJ_SP.parquet` |
| `--category tourism=hotel` | `tourism_hotel.parquet` |
| *(no filters)* | `poi_export.parquet` |

### Available OSM categories

Key categories present in the dataset (see `app/cnae_osm_map.yaml` for the full list):

| OSM Key | Values |
|---------|--------|
| `amenity` | `fuel`, `restaurant`, `fast_food`, `bar`, `bank`, `pharmacy`, `hospital`, `clinic`, `dentist`, `school`, `university`, `cinema`, `parking`, `car_wash`, `place_of_worship`, `police`, `bus_station`, `taxi`, `library`, `museum`, `laundry`, `nursing_home`, `veterinary`, `post_office` |
| `shop` | `supermarket`, `convenience`, `bakery`, `butcher`, `seafood`, `clothes`, `shoes`, `jewelry`, `electronics`, `computer`, `hairdresser`, `beauty`, `optician`, `pet`, `florist`, `sports`, `books`, `hardware`, `furniture`, `car`, `car_repair`, `car_parts`, `motorcycle` |
| `tourism` | `hotel`, `motel`, `hostel`, `guest_house`, `travel_agency` |
| `leisure` | `fitness_centre`, `sports_centre`, `cinema`, `amusement_arcade`, `zoo` |
| `office` | `lawyer`, `accountant`, `architect`, `engineer`, `real_estate`, `insurance`, `it`, `advertising` |
| `craft` | `electrician`, `plumber`, `carpenter`, `shoemaker`, `jeweller`, `electronics_repair` |
| `healthcare` | `physiotherapist`, `psychologist`, `nutritionist`, `rehabilitation` |

Records with no CNAE mapping have `osm_category = null` and can be filtered with:
```bash
python -m app.export_filter --category amenity /mnt/disk1/  # only mapped amenity records
```

---

## Pipeline Steps

| Step | Script | Input | Output |
|------|--------|-------|--------|
| 0 | `download.py` | RF index page | `data/input/YYYY-MM/*.zip` |
| 1 | `step_01_filter.py` | Raw zips | `data/intermediate/step_01_estab_YYYY-MM.parquet` + `step_01_empresas_YYYY-MM.parquet` |
| 2 | `step_02_join.py` | Step 1 Parquet + lookup zips | `data/intermediate/step_02_poi_joined_YYYY-MM.parquet` |
| 3 | `step_03_export.py` | Step 2 Parquet | `data/output/YYYY-MM/poi.{parquet,csv.gz,jsonl,geojson}` |

Each step checks for its output before running — re-running is safe (idempotent).

---

## Applied Filters

- **Active businesses only**: `situacao_cadastral = '02'`
- **No MEI**: `porte != '05'` AND `natureza_juridica != '2135'`

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `duckdb` | Reads zipped CSVs directly, performs joins and exports |
| `requests` | HTTP downloads |
| `tqdm` | Download progress bars |
| `pandas` | GeoJSON serialization |
| `pyyaml` | Config file parsing |

---

## Next Steps

- **Geocoding phase**: add `app/step_04_geocode.py` that reads `step_02_poi_joined_*.parquet`
  and writes lat/lon coordinates to `data/cache/geocode_cache.json` (with incremental saves).
- **GeoJSON enrichment**: `step_03_export.py` will populate `geometry` once geocoding is done.
