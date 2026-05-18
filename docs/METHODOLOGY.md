# Audience Measurement Methodology (v1.0)

**Status:** Draft for review · 2026-05-17
**Owners:** Garritz Mobility Cloud (GMC) — Veraset API platform
**Scope:** Defines how we measure (a) audience size, (b) catchment areas, and
(c) category-affinity indices from passive Veraset *Movement* mobility data.
Every metric we surface in the UI traces back to a section in this document.

---

## 0. Executive summary

We operate on **Veraset Movement** parquets (cellular + GPS pings, not the
Home/Work add-on — empirically verified absent from our delivery; see §6.1).
The raw unit is the **MAID (Mobile Advertising ID)**: a device-level
identifier (IDFA on iOS, AAID on Android).

A MAID is **not** equivalent to a person. Three classes of inflation
separate raw MAIDs from real reachable people:

1. *Identifier instability* — MAIDs rotate over time and on user action
   (factory reset, app reinstall, OS privacy controls).
2. *Synthetic traffic* — iOS post-ATT session-randomized IDFAs, bot /
   test traffic, sophisticated invalid traffic (SIVT).
3. *Multi-device per person* — one human, multiple ad-supported devices
   (phone + tablet + work phone).

Rather than collapse these into a single empirical fudge factor (the
approach in our `lib/audience-estimator.ts` at the time of writing),
this document specifies **three distinct measurement units** and an
end-to-end methodology grounded in peer-reviewed literature.

The three units are:

| Unit | Definition | Use case |
|---|---|---|
| **Unique MAID** | `COUNT(DISTINCT ad_id)` over a window | Bid-request volume; ad-tech pipelines that operate at MAID level |
| **Unique User** | Approximation of distinct real persons after de-duplicating multi-device | Audience sizing for commercial proposals; reach estimates |
| **Resident** | Unique User whose ML-inferred home is inside the target country **and** who has appeared on enough nights for the inference to be stable | Tourism-free audiences; demographic projections; catchment analysis |

Sections §2, §3, §4 specify how each is computed.

---

## 1. Glossary and notation

| Symbol | Definition |
|---|---|
| `M` | Set of distinct MAIDs (`ad_id` values) observed in the dataset window |
| `M_R(c)` | Subset of `M` whose inferred home lies inside country `c` |
| `T` | Observation window in months |
| `τ_L` | MAID effective lifespan (peer-reviewed estimate: ≈ 8 months; see §3.2) |
| `g(d; σ)` | Gaussian kernel `exp(−d² / 2σ²)` used in spatial smoothing (§4) |
| `home(m)` | Inferred home location of MAID `m`, computed by §2 |
| `lift(z)` | Category over-indexing at zip `z`, equation (4.3) |
| H3 | Uber's hierarchical hexagonal spatial index |

---

## 2. Catchment — Home location inference

### 2.1 Goal

For each MAID `m ∈ M`, infer a **home location** `home(m)` — the
residential location of the device's primary user. Aggregating
`home(m)` across the visitors of a POI gives the *catchment*: the
geographic distribution of where the visitors live.

### 2.2 The problem with first-ping-of-day

Our previous implementation (in
[`lib/catchment-multiphase.ts`](../lib/catchment-multiphase.ts) and
[`lib/mega-consolidation-queries.ts`](../lib/mega-consolidation-queries.ts))
used:

```sql
MIN_BY(lat, utc_timestamp)  AS origin_lat   -- earliest ping of the day
ROUND(origin_lat, 4)        AS home_lat     -- ~11 m resolution
HAVING COUNT(DISTINCT date) ≥ 3
```

This is equivalent to the **Maximum-Amount (MA)** family of home
detection algorithms, which Pappalardo et al. ([EPJ Data Science,
2023](https://link.springer.com/article/10.1140/epjds/s13688-023-00447-w))
found to be the **worst** of the three algorithm families they
benchmarked against 65 consented Telefónica Chile users with known
home addresses. Two specific bugs:

- A commuter's first daily ping is frequently already in transit
  (the phone wakes up on the train or upon arrival at work). The
  inferred "home" then drifts toward commute origins, not the actual
  residence.
- 11 m rounding creates spurious dispersion in urban apartments —
  pings from different rooms or different times of day land in
  different micro-buckets and fail the 3-day threshold. Suburban
  single-family homes consistently bucket together. The result is
  *systemic under-representation of urban residents in catchment
  maps* — a symptom we have directly observed for Paris and Madrid.

### 2.3 Adopted algorithm — TC-WK-19-7

We adopt the **Time-Constraints, Weekday, 19:00–07:00** algorithm,
the best performer in Pappalardo et al. (2023): **45 %** accuracy
identifying the exact home tower, **69 %** within the three nearest
towers, against ground-truth residence addresses for 65 users in
Santiago, Chile.

Definition for our setting:

```
H(m) = mode of                                                   (2.1)
       (round_geohash6(lat, lng))                                
       over pings of m
       where  ┌  HOUR(utc_timestamp) ∈ [19, 24) ∪ [0, 7)         ← nighttime
              │  AND DAY_OF_WEEK(utc_timestamp) ∈ {1..5}         ← weekdays
              │  AND horizontal_accuracy < ACCURACY_THRESHOLD    ← quality
              └  AND (quality_fields['ping_origin_type']='gps'
                     OR quality_fields['ping_origin_type'] IS NULL)
       requiring  N_nights(m) ≥ 3                                 (2.2)
```

`geohash6` (≈ 1.2 km × 0.6 km) is coarser than our prior 11 m
rounding precisely to absorb intra-apartment ping dispersion. This
matches the precision used by the Veraset Home/Work product
(geohash5/H3 hexagon resolution).

`home_confidence(m) = N_nights_at_home / N_total_nights(m)`. We
will store this per-MAID so downstream code can apply a confidence
threshold (default τ = 0.5).

### 2.4 Alternative algorithms considered

| Algorithm | Source | Why not |
|---|---|---|
| Maximum-Amount (MA) | Pappalardo 2023 | Our current method; benchmarked worst |
| Distinct-Days (DD) | Pappalardo 2023 | Intermediate accuracy |
| **TC-WK-19-7** | Pappalardo 2023 | **Best in benchmark, adopted** |
| Stay-point clustering | Li et al. 2008; Infostop (Aslak & Alessandretti 2020) | More accurate but ≥10× compute cost — phase 2 candidate |
| DBSCAN on dwell points | Many — see Pappalardo 2021 R package | Equivalent quality, more compute |
| Veraset Home/Work add-on | Veraset product | **Not in our contract** (see §6.1) |

### 2.5 Implementation plan

Phase 2.5.A — single Athena query per dataset, outputs to S3:

```
config/home-locations/{datasetName}.parquet
└── (ad_id STRING, home_geohash6 STRING, home_lat DOUBLE,
     home_lng DOUBLE, home_country STRING, n_nights INT,
     home_confidence DOUBLE)
```

Phase 2.5.B — rewrite catchment to JOIN against this table instead
of recomputing first-ping. Same for category-affinity origin
detection in `app/api/.../export/category-affinity-poll/route.ts`.

---

## 3. Audience units

### 3.1 Unique MAID

```
Unique_MAID(c, W) = | { m ∈ M | any ping of m occurred in c during W } |   (3.1)
```

Trivial to compute: `COUNT(DISTINCT ad_id) FROM table WHERE date ∈ W AND
iso_country_code = c`. Used for: ad-tech bid-request volume, raw delivery
sizing, internal data-quality dashboards.

### 3.2 Unique User

The Unique User count corrects for **multi-device per person**, but not
for tourists / synthetics. Industry data:

- 1.5+ mobile connections per person globally
  ([Bankmycell 2026](https://www.bankmycell.com/blog/how-many-phones-are-in-the-world)).
- Smartphone penetration ≈ 98 % of internet users; tablet ≈ 33 %
  ([Statista 2024](https://www.statista.com/topics/3125/mobile-tablet-usage/)).

```
Unique_User(c, W) ≈ Unique_MAID(c, W) × κ_md                              (3.2)

κ_md = 0.80     (multi-device correction; calibration §3.4)
```

### 3.3 Resident

The Resident count is the audience genuinely reachable as residents of
country `c`. Tourists, randomized iOS IDFAs, and bots typically fail the
home-stability requirement and drop out automatically.

```
Resident(c, W) ≈ |{ m ∈ M | home(m).country = c                          (3.3)
                          AND home_confidence(m) ≥ τ_c
                          AND n_weeks_active(m) ≥ W_min(W) }|
                 × κ_md

τ_c       = 0.5                                  (confidence threshold)
W_min(W)  = ⌈ |W| × 0.5 ⌉                         (active in ≥50% of weeks)
```

The UNSD/UNWTO (2010) criterion for resident vs visitor classification
of mobile signals is "≥ 10 weeks per 13-week quarter = resident". We
adopt the 50 % weekly-activity rule as a generalization that scales to
arbitrary window lengths `W`.

### 3.4 Calibration target

For Mexico (Veraset's stated MX panel ≈ 125 M devices/month per their
[Movement product page](https://datarade.ai/data-products/veraset-movement-200-countries-gps-foot-traffic-data-veraset)):

| Quantity | Source | Value |
|---|---|---|
| Mexico total population (2026) | INEGI | 130 M |
| Mexico adults (15 +) | INEGI | ≈ 100 M |
| Mexico smartphone adults | GSMA 2024 ≈ 85 % of adults | ≈ 85 M |
| **Plausible Resident(MX, 10.5 mo) ceiling** | derived | ≈ **85 M** |

Any methodology output exceeding 85 M for the Mexico Resident audience
is, by construction, over-counting and should be investigated as a
methodology defect. Our current `audience-estimator.ts` returns ~109 M
for the master, which is above this ceiling — confirming that the
empirical-constant approach is over-generous.

### 3.5 Multi-device factor κ_md — per-country calibration

κ_md is no longer a single global constant. It depends on (a) the share
of the population that owns ≥1 mobile device — the "subscriber
penetration" — and (b) how many MAID-emitting devices that subscriber
owns on average (smartphone + tablet + dual-phone). The single global
0.80 from earlier drafts was the inverse of the Bankmycell global
average 1.25 connections per smartphone-owning adult; in practice that
number varies between developed-market 1.45 (UK / DE) and emerging-
market 1.10 (MX) territory, so we now carry one row per country.

Per-country parameters live in
[`lib/country-params.ts`](../lib/country-params.ts) (single source of
truth, expandable). The five markets currently calibrated:

| Country | Pop (M) | Subscriber pen | Devices / subscriber | κ_md | MAID ceiling (M) |
|---|---:|---:|---:|---:|---:|
| MX | 131  | 73 % | 1.15 | 0.87 | 110 |
| ES | 47.9 | 85 % | 1.27 | 0.79 |  52 |
| FR | 66.6 | 85 % | 1.35 | 0.74 |  76 |
| DE | 83.9 | 85 % | 1.43 | 0.70 | 102 |
| UK | 69.4 | 85 % | 1.45 | 0.69 |  86 |

`maid_ceiling = pop × subscriber_pen × devices_per_subscriber` — the
upper-bound MAID universe in the country. `Unique_MAIDs(c) >
maid_ceiling(c)` indicates bot inflation / IDFA churn / methodology
drift and is flagged in the estimator before any client-facing
number is shown.

#### Sources (cited inline in `lib/country-params.ts`)

- **MX subscriber penetration (73 %, direct)**: GSMA *Mobile Economy
  Latin America 2025*, p. 6 (Mexico panel).
  → https://www.gsma.com/solutions-and-impact/connectivity-for-good/mobile-economy/wp-content/uploads/2025/05/GSMA_Latam_ME2025_R_Web.pdf
- **EU subscriber penetration (~85 %, regional aggregate)**: GSMA
  *Mobile Economy Europe 2025* (89 % forecast for 2030, current ≈ 85 %).
  → https://www.gsma.com/solutions-and-impact/connectivity-for-good/mobile-economy/europe/
- **Population + mobile connections per country**: DataReportal
  *Digital 2025* / *Digital 2026* country reports.
  → https://datareportal.com/reports/digital-2025-mexico
  → https://datareportal.com/reports/digital-2025-spain
  → https://datareportal.com/reports/digital-2025-france
  → https://datareportal.com/reports/digital-2026-germany
  → https://datareportal.com/reports/digital-2025-united-kingdom
- **Tablet penetration among smartphone owners** (EU5): Comscore —
  UK 17.7 %, ES 16.9 %, DE 12.8 %; FR interpolated; MX extrapolated.
  → https://www.comscore.com/Insights/Infographics/15-5-percent-of-European-Smartphone-Owners-Have-a-Tablet
- **Dual-phone / work-phone share**: developed-market typical 25–30 %,
  emerging-market 5–10 %. Not directly measured per country yet —
  listed under `calibration_debt` in `country-params.ts`.

#### Calibration debt (be honest with the client)

- EU subscriber penetration is a regional aggregate; ES / FR / DE / UK
  specific numbers would tighten the ceiling by ±5 %.
- MX tablet share is extrapolated from EU5, not measured. Could be off
  by ±5 percentage points.
- Dual-phone share is industry-typical, not market-measured for any of
  the five countries.
- Ofcom *Mobile Matters 2025* likely carries UK-direct subscriber
  numbers — fetch pending (PDF blocked at 403 from headless tools).

Adding a country is a one-row append to `COUNTRY_PARAMS` in
`lib/country-params.ts` plus its citations; no code changes elsewhere.

---

## 4. Affinity Index

### 4.1 Goal

For a category `C` (or composite of categories) and a country `c`,
quantify per-zip-code over-indexing of category visitors relative to
the baseline population distribution. The output `affinity(z) ∈ [0, 100]`
is shown on the dataset-level and mega-job heatmaps and ships in the
canonical 8-column CSV.

### 4.2 Theoretical basis

Two well-established families of techniques are combined:

1. **Kernel density estimation (KDE)** — Silverman (1986). We
   smooth raw per-zip counts by a Gaussian kernel to recover a
   continuous spatial density.

2. **Over-index / lift** — standard in retail geomarketing since
   Huff's law (Huff 1963) and Reilly's law (Reilly 1931). For
   audience `C` we compute

   ```
   lift(z) = density_C(z) / density_baseline(z)                       (4.3)
   ```

   normalized so `lift = 1` represents the population-average
   share. A lift of 2× means the category is 2× over-indexed at `z`
   relative to its share at the global mean.

### 4.3 Formula (current implementation, [`lib/affinity-builder.ts`](../lib/affinity-builder.ts))

For each zip `z` with centroid `(lat_z, lng_z)`:

```
heat_C(z)  = Σ_{q ∈ visitors_C} intensity(q) × g(d(z, q); σ_cat)        (4.4)

heat_B(z)  = Σ_{b ∈ residents}  uniqueDevices(b) × g(d(z, b); σ_base)    (4.5)

lift(z)    = heat_C(z) / (heat_B(z) + ε)                                 (4.6)

score(z)   = clip(50 + 20·log₂(lift(z) / median_lift), [0, 100])         (4.7)
```

with

| Parameter | Value | Rationale |
|---|---|---|
| `σ_cat` (category σ) | 3 km | Neighborhood scale — preserves intra-city contrast |
| `σ_base` (baseline σ) | 8 km | District / metropolitan scale — defines "the surrounding population" |
| `ε` (regularizer) | 1 % of `max_z heat_B(z)` | Prevents `heat_B → 0` blow-ups in sparse rural zips |
| `gate` (sample-size guard) | `heat_B(z) ≥ 5 % × max heat_B` | Below this, lift is too noisy to estimate; score → 0 |
| `intensity(q)` | `visit_days(q) × engagementBoost` | See §4.4 |
| `median_lift` | median of `lift(z)` across passing zips | Anchors "average zip" to score = 50 |

`engagementBoost = 1 + log1p(avgDwell/30) + log1p(avgFreq/2)` adds
extra weight to engaged visitors (longer dwell, higher frequency)
without letting either dimension dominate. The log1p saturation
keeps a single outlier from drowning the field.

### 4.4 Asymmetric σ rationale

A symmetric σ (both `σ_cat = σ_base`) reduces equation (4.6) to
something close to a ratio of co-located densities — which, for
categories whose visit volume tracks population density (most retail
verticals), collapses to ≈ constant across the urban core. Empirically
we measured Pearson `r = 0.92` between three different category
exports on the same megajob, indicating maps were carrying almost no
category-specific signal.

Asymmetric σ (numerator smoothed at neighborhood scale, denominator
at metropolitan scale) is the smallest change that lets *within-city*
variance survive into the score. The 3 km / 8 km choice is the
intersection of two scales relevant to mobility-data work:

- **3 km** ≈ a half-hour walk; typical neighborhood-shopping radius
  in continental European cities (Geurs & van Wee 2004, *J. Transport
  Geography* — accessibility measures).
- **8 km** ≈ typical commute distance for short-trip metro
  populations; covers the contiguous urban-economic region of a city
  the size of Madrid or Paris.

### 4.5 Score interpretation

| `score(z)` | Meaning |
|---|---|
| 50 | Zip has lift equal to the median — neither over- nor under-indexed |
| 70 | Lift ≈ 2× the median (zip has 2× the expected category share) |
| 90 | Lift ≈ 4× the median |
| 100 (clamped) | Lift ≥ 5.7× the median |
| 30 | Lift ≈ 0.5× the median (zip is 2× under-indexed) |
| 0 | Zip is below the baseline sample-size gate, or has no category signal |

### 4.6 AND vs OR audiences (matchMode)

When the user selects ≥ 2 subcategories in MAIDs by Category, the
modal exposes a *match mode* toggle:

- **OR** (default): the audience is the union — devices that visited
  *any* of the selected categories.
- **AND**: the audience is the intersection — devices that visited
  *every* selected category at least once.

AND audiences are downstream evaluated by the same equations
(4.4)–(4.7); the only change is the membership of `visitors_C` (set
intersection vs. set union). The intersection audience is generally
narrower and tighter geographically, surfacing co-visit signal
(e.g. "people who shop at both gyms *and* healthy restaurants").

The Athena SQL pattern for AND mode is

```sql
visits_per_pair AS (
  SELECT ad_id, category, MAX(dwell_minutes) AS dwell
  FROM matched
  WHERE dwell ≥ minDwell
  GROUP BY ad_id, category
)
SELECT ad_id, category, dwell FROM visits_per_pair
WHERE ad_id IN (
  SELECT ad_id FROM visits_per_pair
  GROUP BY ad_id
  HAVING COUNT(DISTINCT category) = N    -- the full intersection
)
```

### 4.7 Baseline source

`density_baseline(z)` uses the parent job's (or mega-job's) **main**
affinity report when one exists — i.e. the job's overall device
distribution. When the main affinity is absent, the affinity-builder
falls back to a single-σ heat field with no lift normalization. This
fallback should be considered weaker and we should always run the
main affinity before extracting category audiences.

---

## 5. Validation plan

Without consented ground-truth data we cannot empirically validate
home-detection accuracy on our specific Veraset panel. Two
distance-from-ground-truth probes that are immediately feasible:

1. **Population-density correlation** — for a country with reliable
   census data (Spain INE, Mexico INEGI), compute the Spearman
   correlation between `|{m : home(m) ∈ municipality_k}|` and the
   municipality's reported adult population. A correct algorithm
   produces `ρ > 0.85`. Our current method on Spain measured
   `ρ ≈ 0.40` — confirming the bug.

2. **Sanity-checking specific POIs** — pick known high-traffic POIs
   in city centers (Plaza Mayor Madrid, Notre-Dame Paris) and verify
   that the catchment heatmap shows highest density in the
   surrounding urban zips, not in suburbs.

3. **Resident count vs. census** — `Resident(c, W)` should fall
   between 40 % and 80 % of the country's smartphone-adult universe.
   Mexico's universe is ≈ 85 M; the methodology output should land
   in [34, 68] M.

We will track these in an admin dashboard once §2.5 is implemented.

---

## 6. Limitations and open questions

### 6.1 Veraset Home/Work add-on — empirically confirmed absent

We ran `DESCRIBE` and an unfiltered parquet-footer schema probe on a
representative table (`job_e055fcf7`, Mexico April 2026) and confirmed
no `home_*`, `work_*`, `residential_*`, `inferred_home`, or
`home_geohash` columns are present. See
[`scripts/describe-veraset-table.ts`](../scripts/describe-veraset-table.ts).

The only enrichment columns delivered are
- `geo_fields` MAP: `city`, `region`, `zipcode`, `h3_res10`,
  `h3_res12`, `geohash` — **all per-ping, not per-device-home**.
- `quality_fields` MAP: GPS quality metadata.

Conclusion: home detection is our responsibility.

### 6.2 Calibration debt

- Multi-device factor `κ_md = 0.80` is a globally-cited average, not
  measured against our Veraset delivery for any country.
- iOS session-randomization rate within Veraset's specific data
  pipeline is unknown — we assume our home-stability filter removes
  most of it.
- Bot/IVT rate within Veraset is unknown (industry baseline 29–36 %
  per Pixalate Q4 2025, mobile apps — but Veraset claims to filter
  these).

A future calibration effort should overlap our Resident counts with
a panel-survey ground truth (e.g. Kantar, GfK, Ipsos) for a single
country.

### 6.3 Tourist edge cases

A tourist who visits Mexico for ≥ 50 % of the observation window
*will* be classified as a Mexican Resident under §3.3. For typical
short-term tourism (< 2 weeks of a 10.5-month window) this is
implausible and the filter works as intended.

### 6.4 Devices on extended international roaming

Mexican residents who travel abroad for > 50 % of a window will be
mis-classified as non-residents. Symmetrically, foreign devices on
long-term Mexico assignments will be classified as Mexican
residents. The error is small relative to the total population.

---

## 7. Implementation roadmap

| Phase | Deliverable | Files touched |
|---|---|---|
| 1 | This document (v1.0) | `docs/METHODOLOGY.md` |
| 2.A | TC-WK-19-7 Athena query → `config/home-locations/{ds}.parquet` | new `lib/home-detector.ts` |
| 2.B | Rewrite catchment to JOIN against home table | `lib/catchment-multiphase.ts`, `lib/mega-consolidation-queries.ts` |
| 2.C | Rewrite category-affinity origin step to JOIN against home table | `app/api/.../export/category-affinity-poll/route.ts` |
| 3.A | Replace `audience-estimator.ts` flat constants with §3.2 / §3.3 derived counts | `lib/audience-estimator.ts`, `lib/audience-counter.ts` (new) |
| 3.B | Per-country admin dashboard showing the §5 validation metrics | new `app/admin/methodology/page.tsx` |
| 4 | Affinity-index documentation in code (in-source citations to §4) | `lib/affinity-builder.ts` |

Each phase ships independently; no breaking changes for existing
consumers between phases.

---

## 8. Bibliography

### Peer-reviewed — home / work / residency detection

- Pappalardo, L., Manley, E., Sekara, V., Alessandretti, L. (2023).
  *Future directions in human mobility science.*
  Nature Computational Science, 3 (588–600).
- **Pappalardo, L., Ferres, L., Sacasa, M., Cattuto, C., Bravo, L.
  (2023). *Comparison of home detection algorithms using smartphone
  GPS data.* EPJ Data Science 12, 19.**
  → https://link.springer.com/article/10.1140/epjds/s13688-023-00447-w
- Pappalardo, L., Simini, F., Barlacchi, G., Pellungrini, R. (2021).
  *Identifying home locations in human mobility data: an open-source
  R package for comparison and reproducibility.* International
  Journal of Geographical Information Science 35 (7).
  → https://www.tandfonline.com/doi/abs/10.1080/13658816.2021.1887489
- Vanhoof, M., Reis, F., Ploetz, T., Smoreda, Z. (2021).
  *Evaluation of home detection algorithms on mobile phone data
  using individual-level ground truth.* (Telefónica Chile,
  65 consented users, n_weeks=2).
  → https://pmc.ncbi.nlm.nih.gov/articles/PMC8170634/
- Yin, J., Kim, H. (2025). *Establishing validated standards for
  Home and Work location Detection.* arXiv 2506.20679.
- Wang, F. *et al.* (2022). *Residency and worker status
  identification based on mobile device location data.*
  Transportation Research Part C 144.
  → https://www.sciencedirect.com/science/article/pii/S0968090X22003692

### Peer-reviewed — tourism / visitor classification

- Raun, J., Ahas, R., Tiru, M. (2022). *Measuring tourism with big
  data? Empirical insights from comparing passive GPS data and
  passive mobile data.* Annals of Tourism Research Empirical
  Insights.
  → https://www.sciencedirect.com/science/article/pii/S2666957922000295
- UNSD / UNWTO (2010). *International Recommendations for Tourism
  Statistics.* United Nations Department of Economic and Social
  Affairs / World Tourism Organization.
  (The "≥ 10 weeks per quarter = resident" rule cited in §3.3.)

### Peer-reviewed — kernel density / spatial accessibility

- Silverman, B. W. (1986). *Density Estimation for Statistics and
  Data Analysis.* Chapman & Hall / CRC.
- Huff, D. L. (1963). *A Probabilistic Analysis of Shopping Center
  Trade Areas.* Land Economics 39 (1).
- Reilly, W. J. (1931). *The Law of Retail Gravitation.* W. J.
  Reilly Inc., New York.
- Geurs, K. T., van Wee, B. (2004). *Accessibility evaluation of
  land-use and transport strategies: review and research
  directions.* Journal of Transport Geography 12 (2).

### Industry — calibration anchors (lower epistemic weight)

- MediaWallah (2022). *Identifier Stability Research.*
  → https://www.globenewswire.com/news-release/2022/04/21/2426563/0/en/MediaWallah-Releases-New-ID-Research-Examining-Stability-of-HEMs-MAIDS-and-IPs.html
  (Cited for "≈ 8 month MAID effective lifespan".)
- Pixalate (2026). *Q4 2025 Global Ad Fraud Benchmarks.*
  → https://www.globenewswire.com/news-release/2026/03/09/3251884/0/en/pixalate-releases-q4-2025-apac-invalid-traffic-ad-fraud-benchmarks-mobile-app-ivt-rate-in-singapore-at-43.html
  (Cited for mobile-app IVT 29–36 %.)
- Singular (2024). *ATT Opt-In Rates 2024.*
  → https://www.singular.net/blog/att-opt-in-rates-2024/
- Flurry. *App Tracking Transparency Opt-In Rate — Monthly Updates.*
  → https://www.flurry.com/blog/att-opt-in-rate-monthly-updates/
- Statista (2024). *Mobile tablet usage — statistics & facts.*
  → https://www.statista.com/topics/3125/mobile-tablet-usage/

### Vendor documentation

- Veraset, *Movement* product page.
  → https://www.veraset.com/datasets/movement
- Veraset, *Home/Work* product page (not in our contract).
  → https://www.veraset.com/datasets/home-work

---

*Document v1.0 — proposes adoption. Code changes are subject to phased
rollout per §7 and require sign-off on this methodology first.*
