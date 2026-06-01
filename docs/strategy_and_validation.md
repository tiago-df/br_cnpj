# Geocoding Strategy & Validation Log
## BR CNPJ → POI Pipeline — Step 04a: APT Local Geocoding

**Data dump:** 2026-05-10  
**Target dataset:** 3,754,440 active POIs (all Brazil) / 982,079 POIs (São Paulo)  
**Primary source:** TomTom Orbis Address Points GeoPackage (`Orbis_Address_Points_BRA_SP.gpkg`)  
**Last updated:** 2026-06-01

---

## 1. Problem Statement

The CNPJ dataset from Receita Federal contains ~3.75M active business establishments
with postal addresses (logradouro, número, CEP, bairro, município) but **no geographic
coordinates**. The geocoding pipeline must assign lat/lon to each POI with the highest
possible precision (house-number level) while minimising cost and API dependency.

### Address data quality in CNPJ

| Issue | Prevalence |
|---|---|
| CEP inválido ou desatualizado | ~15–20% dos registros |
| Número como `S/N` ou `0` | ~12% |
| Nome de rua abreviado ou inconsistente | ~25% |
| Bairro divergente entre CNPJ e outras bases | ~30% |
| Endereços em CEPs rurais de cobertura enorme | ~5% |

---

## 2. Approach: Local Join with TomTom Orbis Address Points

Instead of calling a geocoding API for every record, we perform a **local join**
between the CNPJ address fields and the TomTom Orbis Address Points GeoPackage —
the same underlying data used by the TomTom Search API. This allows house-number
precision geocoding at zero API cost for matched records.

### Pre-processing

The GeoPackage is converted once to a normalised Parquet index (`apt_index_*_v2.parquet`):

- `cep_norm`: digits only (8-char)
- `num_norm`: leading digits, NULL for S/N
- `street_norm`: accent removal + uppercase + abbreviation expansion (Python UDF)
- `suburb_norm`: same normalisation as street; `suburb_clean` additionally strips
  parenthetical suffixes like `(ZONA NORTE)`
- `city_norm`: accent removal + uppercase

POI normalisation uses pure SQL (CNPJ data is ASCII uppercase):
- `bairro_norm`: strip parenthetical suffixes + remove prepositions + collapse whitespace

---

## 3. Join Layers — Design and Evolution

### Layer 1 — `apt_cep_num_street`

**Logic:** `CEP = CEP AND num = num AND jaro_winkler(street_poi, street_apt) >= threshold`

The CEP acts as a geographic anchor. The Jaro-Winkler similarity handles minor spelling
differences in street names (accents, abbreviations not caught by normalisation).

### Layer 2 — `apt_cep_num` *(removed — see § 5.1)*

**Logic:** `CEP = CEP AND num = num` — unique match only

### Layer 3 — `apt_street_exact`

**Logic:** `street = street AND num = num AND city = city`, bairro JW disambiguation

Used for POIs whose CEP is missing or invalid. Exact street name match within city,
with bairro Jaro-Winkler (≥ 0.80) to disambiguate when the same street name exists
in multiple neighbourhoods.

### Layer 4 — `apt_fuzzy` *(removed — see § 5.2)*

**Logic:** `jaro_winkler(street_poi, street_apt) >= threshold` within city + bairro blocking

---

## 4. Validation Methodology

### 4.1 TomTom API Comparison (300-POI stratified sample)

A stratified random sample of 300 geocoded POIs (proportional to layer coverage)
was re-geocoded via the **TomTom Structured Geocode API**. Haversine distance between
the APT join result and the API result was computed for each POI.

The TomTom Search API uses the same TomTom Orbis data as our GeoPackage (different
vintage/representation), making it a meaningful but not fully independent reference.

**Script:** `app/step_04a_validate.py`  
**Output:** `data/output/apt_validation_300_2026-05-10.csv`

### 4.2 Resample (zero API calls)

After each change to the join logic, the same 300 CNPJs were re-geocoded in-process
using the updated logic, and haversine distances were recomputed against the stored
TomTom API coordinates. This allowed rapid iteration without additional API spend.

**Script:** `app/step_04a_resample.py`

### 4.3 OSM / Nominatim Independent Validation

For the 33 L3+L4 cases with distance > 50 m vs TomTom, each address was geocoded
via **Nominatim (OpenStreetMap)** as a fully independent third reference. Distances
to APT and TomTom were computed; the closer result was declared the winner.

---

## 5. Decision Record

### 5.1 — Remove Layer 2 (`apt_cep_num`) ✅

**Date:** 2026-06-01  
**Decision:** Permanently removed.

**Rationale:**  
Layer 2 matched by CEP + house number only, requiring a unique match within the CEP
zone. This is unsafe because:

- In small cities, a single CEP covers all streets; house number N can exist on
  many different streets simultaneously.
- Validation sample had 4 L2 records, one with a **111 km error**
  (`RUA VINTE E TRES DE MAIO, Jundiaí, num=500, CEP=11740000`) — the same number
  existed on a completely different street 111 km away.

**Impact:** −1.2% coverage (11,375 POIs from SP). These migrate to Phase C (TomTom API).

---

### 5.2 — Remove Layer 4 (`apt_fuzzy`) ✅

**Date:** 2026-06-01  
**Decision:** Permanently removed.

**Rationale:**  

The layer used Jaro-Winkler ≥ 0.85 on the street name within city + bairro blocking
to find approximate street matches for POIs without a valid CEP. Multiple rounds of
validation and tuning were applied before the removal decision:

| Iteration | Change | L4 >5 km rate |
|---|---|---|
| v1 original | left(5) prefix blocking | 50% |
| v2 | L1 threshold 0.90, remove L2 | 37% |
| v3 | bairro prefix left(4) blocking | 34% |
| v4 | L3 bairro JW>=0.80, L4 full street JW | 27% |

Even after all optimisations, 27% of L4 matches were > 5 km off (TomTom reference).

**OSM validation (definitive):**  
33 L3+L4 cases with APT↔TomTom distance > 50 m were independently geocoded via
Nominatim (OpenStreetMap). For the fuzzy layer:

| Result | Count | % |
|---|---|---|
| TomTom closer to OSM (**TomTom correct**) | 22 | 92% |
| APT closer to OSM (**APT correct**) | 2 | 8% |
| No OSM result | 4 | — |

**Conclusion:** 92% of fuzzy matches are false positives — the APT join finds a
similarly-named street in a different neighbourhood. This is a structural problem:
without a CEP anchor, Jaro-Winkler similarity alone cannot reliably distinguish
homonymous streets across a large city like São Paulo.

**Impact:** −10.2% coverage (100,100 POIs from SP). These migrate to Phase C (TomTom API),
which correctly resolves the vast majority (confirmed by OSM validation).

---

## 6. Test Results Summary

### 6.1 SP geocoding — final results (v5, L1+L3 only)

**Run date:** 2026-06-01 | **POIs:** 982,079 | **APT:** 12,316,667 rows

| Layer | POIs | Coverage | Mean dist vs TomTom |
|---|---|---|---|
| `apt_cep_num_street` (L1) | ~597K | ~60.9% | 23 m |
| `apt_street_exact` (L3) | ~78K | ~7.9% | 945 m |
| **Total geocoded** | **~675K** | **~68.8%** | — |
| `none` (for Phase C) | ~307K | ~31.2% | — |

*Exact numbers will be updated when the 2026-06-01 run completes.*

### 6.2 Accuracy vs TomTom API (300-POI sample, v5 logic)

| Metric | Value |
|---|---|
| TomTom matched | 300 / 300 (100%) |
| Mean distance (all) | 903 m |
| Median distance | 0 m |
| < 50 m (same block) | 85% |
| > 5 km (suspect) | 4% |

### 6.3 Layer-level accuracy (OSM validation)

| Layer | n tested | APT wins vs OSM | TomTom wins vs OSM |
|---|---|---|---|
| `apt_cep_num_street` | 228 | — | — |
| `apt_street_exact` | 4 (>50m cases) | **3 (75%)** | 1 (25%) |
| `apt_fuzzy` (removed) | 24 (>50m cases) | 2 (8%) | **22 (92%)** |

### 6.4 Layer 1 distance breakdown (v5, 228 sample)

| Range | Count | % | Root cause |
|---|---|---|---|
| = 0 m (identical) | 190 | 83% | Same source data (Orbis) |
| 1–50 m | 25 | 11% | Address point vs road interpolation philosophy |
| 50–500 m | 7 | 3% | Long streets (Av. Ragueb Chohfi), data vintage diff |
| > 500 m | 6 | 3% | Rural/large CEPs (11740000 Itanhaém, 14 APT candidates for same address) |

---

## 7. Change Log

| Commit | Change | Rationale |
|---|---|---|
| `0016218` | Initial APT geocoding, L1–L4, `sim_threshold=0.80` | Baseline |
| `ee2a978` | Chunked processing, checkpoint after every chunk | Resume on interrupt |
| `9575f94` | `null_handling='special'` on DuckDB Python UDFs | Fix NULL crash |
| `3869040` | Pure SQL normalisation for POI side (10× faster) | Performance |
| `6911179` | `step_04a_validate.py` — TomTom API comparison | Validation tooling |
| `bd3710e` | Retry 429 in validate, fix sampling and stats | Fix rate-limit errors |
| `7c6b00e` | L1 threshold 0.80→0.90, remove L2, bairro in L3/L4 | Quality improvement |
| `d4200ff` | Fix SyntaxWarning backslashes in f-strings | Python 3.12 compat |
| `45e2600` | L3 bairro strip parentheses + L4 JW>=0.80 blocking | Bairro normalisation |
| `70d51ac` | Fix remaining SyntaxWarnings (`\d`, `\s`) | Python 3.12 compat |
| `f3d76c9` | L3 bairro JW>=0.80, L4 full-street JW (no prefix) | Quality improvement |
| `0c158b1` | **Remove L4 entirely** | 92% false-positive confirmed |

---

## 8. Planned Next Steps

### Phase B — IBGE CNEFE Join (free, independent source)

**Source:** Cadastro Nacional de Endereços para Fins Estatísticos (IBGE Census 2022)  
**URL:** `ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/`  
**Format:** CSV per municipality, fields: logradouro + número + CEP + lat/lon

**Strategy:** Apply the same **L1 + L3** logic used for Orbis APT to the CNEFE dataset,
targeting only POIs not geocoded in Step 04a (i.e., `geo_precision = 'none'`).

Expected coverage gain: +10–15% of the unmatched POIs.

```
Step 04a  →  L1+L3 on Orbis APT        → ~69% coverage
Step 04b  →  L1+L3 on IBGE CNEFE       → +10–15% of remainder
Step 04c  →  Address deduplication     → +5–10% of remainder
Step 04d  →  TomTom API (Phase C)      → remaining ~20%
```

**Implementation plan:**

1. Download CNEFE for SP (and other states incrementally)
2. Build normalised Parquet index (same schema as Orbis APT `_v2`)
3. Create `app/step_04b_cnefe_geocode.py` re-using `_process_chunk()` from step_04a
4. Validate with the same 300-POI sample + OSM cross-check

### Phase C — Address deduplication

For POIs still unmatched after Phase A+B: if another CNPJ at the exact same
address (same CEP + number + street) was already geocodified, propagate the
coordinates. Targets condomínios comerciais where 20–50 companies share an address.

### Phase D — TomTom API (Structured Geocode)

Remaining unmatched POIs (~20% after A+B+C) sent to TomTom Structured Geocode API.
OSM validation confirmed TomTom correctly resolves the vast majority of cases that
local join misses.

---

## 9. Key Architectural Decisions

| Decision | Rationale |
|---|---|
| Local join before API calls | Eliminates API cost for ~69% of records using same source data |
| DuckDB for all Parquet I/O | No pyarrow dependency; single binary; handles 12M row APT in 2 s |
| Checkpoint after every chunk | Interrupt-safe; 10K-POI chunks = max 10K records lost on crash |
| Python UDFs only on APT side | CNPJ data is already ASCII uppercase; UDFs on 982K rows = 42 s wasted |
| JW threshold 0.90 for L1 | 0.80 allowed rural-CEP false positives (up to 15 km); 0.90 eliminates them |
| Bairro JW >= 0.80 for L3 | Tolerates "JD SAO JOAO" ≈ "JARDIM SAO JOAO"; rejects cross-bairro matches |
| No fuzzy without CEP anchor | OSM validation: 92% false-positive rate for JW-only street matching in large cities |
