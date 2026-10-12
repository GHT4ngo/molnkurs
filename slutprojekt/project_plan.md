# Slutprojekt: project plan

The working script for the whole project: what we build, why, in which order, and what it costs.
Updated as we go, so it describes what exists rather than what was once intended.

Status: G task complete. VG task in progress. See `setup_log.md` for the environment inventory and
`documentation.md` for the hand in.

## 1. The question

Management will place **50 fast charging stations**. They want them where they do the most good:

| Criterion | What it means in data |
|---|---|
| Many EVs | number of battery electric cars in the kommun today |
| Fast growth | change in electric cars per adult over three years |
| Cheap electricity | price level in the kommun's elområde |

**The deliverable is not a pipeline.** It is a ranked list of kommuner with the reasoning behind
the ranking. The pipeline is how we get there, and it has to be re-runnable without duplicates.

## 2. Decisions taken

| Decision | Choice | Why |
|---|---|---|
| Platform | Azure Databricks, workspace `dbw-molnkurs-dev-01` | required by the assignment, set up in Lab 2 |
| Architecture | Medallion: bronze, silver, gold | required by the assignment |
| Storage | External, our own account `stmolnkurst4ngo02`, container `unity` | files stay ours and are browsable in Azure |
| Compute | Serverless notebooks while developing, serverless **jobs** for the pipeline | jobs are half the DBU rate and start in seconds |
| Orchestration | **Azure Data Factory** driving Databricks jobs through the Jobs REST API | required by VG. The REST route keeps serverless, see section 7 |
| Kommun to elområde | Static seed table, documented as an assumption | no public API gives this mapping |
| EV stock | **Measured**, from Trafikanalys `t10026` | the derived estimate failed, see section 5b |
| Charger supply | Kolada, sourced from Energimyndigheten and Nobil | authoritative, 277 of 290 kommuner |
| Language | English in all documentation, except `assignment.md` | course rule |

## 3. Data sources

### In the pipeline

| Source | Gives | Key | Grain |
|---|---|---|---|
| SCB **TAB3277** | new registrations by fuel | no | kommun, month, fuel |
| SCB **TAB3276** | cars in traffic by ownership | no | kommun, year |
| SCB **TAB638** | population by single year of age | no | kommun, year, age |
| SCB **TAB628** | land area and density | no | kommun, year |
| **Elpriset just nu** | electricity price | no | elområde, hour |
| **Trafikanalys `t10026`** | cars in traffic **by fuel** | no | kommun, year, fuel |
| **Kolada v3** | charge points, driving distance, median income | no | kommun, year |
| **NOBIL** | charging sites and connectors, the source Kolada publishes from | yes | site |
| **SMHI metobs** | monthly mean temperature | no | station, month |

`TAB638` was fetched in Lab 2 and reused rather than fetched again.

### Tested and rejected

**Trafikverket `TrafficFlow`.** Live detector readings rather than annual average daily traffic, and
the detectors sit in three metro areas: 19 kommuner of 290, with 327 sites in Salem and 2 in
Nynäshamn. As a ranking component it would have acted as a hidden "Stockholm suburb" flag.
Notebook `07_bronze_trafikverket` is kept as evidence and is deliberately not in the pipeline.

### Parked, with the research recorded

**Lastkajen**, Trafikverket's NVDB delivery service, is the right source for road traffic and would
close the one remaining modelling gap, because NVDB carries **ÅDT**, measured annual average daily
traffic per segment. Findings so far, so this does not have to be rediscovered:

- REST API at `https://lastkajen.trafikverket.se/api/`, which rewrites internally to `/services/api/`.
- Exactly three controllers respond: `identity`, `datapackage`, `file`, plus an OData endpoint.
- `POST /api/identity/Login` exists and expects credentials. Everything past login is invisible
  without a token, so the action names must be discovered once authenticated.
- Expect SWEREF99 TM, EPSG:3006, and Shapefile or GeoPackage rather than JSON.
- Fallback if it proves impractical: Natural Earth `ne_10m_roads`, tested, 629 features over Sweden
  with E-road labels, but no traffic volumes and coarse geometry.
- Overpass is not an option from this environment: 406 and 504 from three endpoints.

**The interactive front end** was unparked and built. `gold.laddstolpe_ranking` and
`gold.utbyggnadsplan` are exported to Azure SQL by `60_export_sql`, and an App Service web app,
`app-molnkurs-t4ngo-02`, reads them over its managed identity and recomputes the ranking, the map and
the whole allocation in the browser as the six weight sliders move. ADF parameters set the weighting
the pipeline records; the sliders explore alternatives without a pipeline run. See section 7.

## 4. Architecture

```
  APIs --> BRONZE --> SILVER --> GOLD --> the ranked list and the 50 locations
           raw        cleaned     star schema
```

| Object | Name |
|---|---|
| Catalog | `slutprojekt`, managed location `abfss://unity@stmolnkurst4ngo02.dfs.core.windows.net/slutprojekt` |
| Schemas | `bronze`, `silver`, `gold` |
| Raw files | external volume `slutprojekt.bronze.raw` |
| Reused from Lab 2 | `dbw_molnkurs_dev_02.scb.tab638` |

| Layer | Rule |
|---|---|
| **Bronze** | as the API returned it, plus `_source`, `_source_file`, `_ingested_at`. Raw responses land in the volume first, so a re-fetch is never needed. |
| **Silver** | typed and cleaned, kommunkod as a four character key with leading zeros, one row per grain, quality checks that fail loudly. |
| **Gold** | the model management reads. Star schema, a ranking table and an allocation table. |

**Why bronze keeps the raw files.** If a transformation is wrong we rebuild from files instead of
hitting the APIs again. It is also what makes automated retry safe: a retried bronze run skips every
file already on disk.

## 5. Data model (gold)

```
        dim_kommun                  dim_tid              dim_elomrade
   kommunkod, kommun, lan       datum, ar, manad,     elomrade, namn
   elomrade, invanare,          kvartal, ar_manad
   invanare_vuxen, areal,
   inkomst, andel_juridiska,
   vintermedel
        |                            |                       |
        +------------+---------------+-----------+-----------+
                     |                           |
              fact_elbilar                 fact_elpris
              fact_laddinfra               (elomrade, datum)
              fact_nyreg
                     |
                     v
            laddstolpe_ranking  --->  utbyggnadsplan
```

**`gold.laddstolpe_ranking`** is one row per kommun with six components and a total score:

| Component | Measure | Scores high when | Because |
|---|---|---|---|
| `otackt_behov` | electric car kilometres per existing fast charge point | **more** km per charge point | demand already outruns supply |
| `volym` | electric cars, log scale | **more** cars | a station there serves more drivers |
| `tillvaxt` | change in electric cars per 1,000 adults over three years | **faster** growth | the fleet is arriving, build ahead of it |
| `transit` | traffic work on the road network, vehicle km per day, log scale | **more** traffic | travellers charge where they drive, not where they live |
| `elpris` | average price in the kommun's elområde | a **lower** price | cheaper power, so the site is cheaper to run |
| `kyla` | winter mean temperature at the nearest SMHI station | a **colder** winter | cold costs a battery about a quarter of its range, so the same journey needs more charging |

**`gold.utbyggnadsplan`** is the answer: 50 stations awarded one at a time over 50 rounds, recomputing
unmet need after every single station, under two caps. One plan is produced per run, on the
weighting the ranking recorded. Three single-component plans were removed once the web app could
produce the same thing live.

**Why a star schema and not one wide table.** The sources have three different grains: kommun and
year, kommun and month, elområde and day. A wide table forces the finest grain on all of them and
repeats every kommun name and coordinate on every row. Dimensions also mean a new source becomes a
new fact table rather than a rebuild, which matters with a VG task still landing.

**Why there is no `fact_fordon`.** SCB `TAB3276` and Trafikanalys `t10026` both report 5,029,589
cars in traffic for 2025, to the car, because SCB republishes Trafikanalys. Two tables claiming to
be the source of one number is worse than one, so the agreement is recorded as a validation and a
single source is used.

### 5b. The EV estimate that was discarded

The first model estimated the electric car stock per kommun by cumulating new registrations from
2015, because SCB has no kommun level table of the fleet by fuel. It did not survive contact with
reality.

| Kommun | Estimated | Actual | Ratio |
|---|---|---|---|
| Sundbyberg | 9,612 | 2,473 | 3.89 |
| Lund | 24,540 | 6,617 | 3.71 |
| Stockholm | 265,109 | 138,763 | 1.91 |

Cars are first registered where the dealer, the leasing company or the corporate head office sits,
and then driven elsewhere. The **median kommun has a ratio of 0.97**, so the estimate was nearly
right for a typical kommun and wrong by up to a factor of four for about 15 of them, including
Stockholm, Malmö and Lund. Company ownership explains only part of it: the correlation between the
ratio and the company owned share is **0.10**.

Replaced by Trafikanalys `t10026`, which publishes cars in traffic per kommun and fuel. The estimate
is kept in `gold.datakvalitet_estimat` as the evidence behind the largest decision in the project.

## 6. Re-runnable without duplicates

**By overwrite, not by merge.** Every layer rebuilds from the layer below and nothing reads its own
output, so a rerun cannot accumulate. `MERGE INTO` would be the tool if bronze arrived incrementally,
which it does not.

| Layer | Mechanism |
|---|---|
| Raw files | skip a file that already exists |
| Bronze, silver, gold | full overwrite from the layer below |
| Allocation | greedy loop, sorted by kommunkod with a stable tie break, so ties resolve identically every run |

**Proven, not asserted.** `40_idempotens` compares each table's current Delta version against its
previous one with `EXCEPT ALL` in both directions, excluding the audit columns whose job is to
change. Nine tables, zero differences, and a separate check confirms no duplicates on any natural
key.

## 7. The pipeline

**Databricks** holds the work, split into four jobs by layer: `sp_bronze`, `sp_silver`, `sp_gold`,
`sp_allokering`. Bronze keeps its internal parallel DAG.

**Azure Data Factory** holds the ordering, the schedule and the error handling, reusing
`adf-molnkurs-dev-01` from Lab 1 so there is no new resource and no idle cost.

```
ADF pl_slutprojekt:  bronze --> silver --> gold --> allokering
                       each via pl_kor_databricks_jobb
                       (Web run-now --> Until poll --> If not SUCCESS --> Fail)
```

**Why ADF calls the Jobs REST API rather than using the Databricks Notebook activity.** The native
activity needs a cluster spec, and every option is classic compute. A new job cluster is created
**per activity**, each taking three to five minutes to start and billed for it, so fourteen
activities would mean roughly an hour of startup for a five minute workload. A Web activity calling
`jobs/run-now` keeps everything on serverless, which starts in seconds.

Authentication is the ADF managed identity, principal `c89e291e-a136-4e3b-b10f-e0f94ca35e7c`,
against Databricks resource id `2ff814a6-3304-4ab8-85cb-cd0e6f879c1d`. The identity must be added to
the workspace and granted run permission on the jobs: the control plane role is not the data plane
role, for the fourth time in this course.

**One weighting, recorded with the answer.** `profil` remains an ADF parameter so every run states
which weighting produced it:

| Component | Weight |
|---|---|
| `behov` | 2.0 |
| `volym` | 1.0 |
| `tillvaxt` | 1.0 |
| `transit` | 1.0 |
| `elpris` | 0.5 |
| `kyla` | 0.5 |

The weights are defined once, in `VIKTER` in `21_gold_ranking`. `30_allokering` reads `_profil` and
`_vikter` back out of the ranking table, and the web app reads the same string from Azure SQL, so
there is no second copy to drift.

**Why the default is not all 1s.** Unmet need is the question being asked, so it gets 2. Volume,
growth and transit get 1 because 50 physical stations serve absolute numbers of cars, the fleet is
still growing fast, and a fast charger serves people passing through as well as residents. Price stays
at 0.5 because it is published per elområde and takes only four values across 290 kommuner, so at
equal weight it behaves as a north versus south switch rather than a measurement. Cold stays at 0.5
for the same reason plus one more: winter temperature is close to a function of latitude, and the
value is borrowed from a weather station that can be tens of kilometres away.

**Schedule: monthly, the 5th at 03:00.** The fastest moving input that changes the answer is SCB's
monthly registration release, published around the 2nd or 3rd. Trafikanalys and Kolada update
yearly, and electricity prices are daily but enter the model as a 12 month average. Twelve runs a
year at about five minutes of serverless job compute is on the order of 0.5 USD per year; a daily
trigger would cost thirty times that and produce the same numbers on twenty nine days out of thirty.

**Error handling.** The Web activity retries twice at 60 second intervals, which covers transient
API failures on top of the retry each notebook already does internally. After that a Fail activity
stops the pipeline with a message naming the layer, the run id and the Databricks run URL. An Azure
Monitor alert on failed pipeline runs sends an email.

## 8. Cost

| | |
|---|---|
| Credit | 100 USD, expires 2027-08-20, spending limit On |
| Spent, 2026-09-25 | **152.35 SEK** month to date, roughly 15.6 USD |
| Remaining | roughly **84 USD** |
| Original target | 30 to 40 USD for the whole project |

Read the currency label before reading the number: budgets report SEK at subscription scope while
the credit is in USD.

**Prices, Premium, Germany West Central**

| Compute | USD per DBU hour |
|---|---|
| Interactive serverless, notebooks | 1.00 |
| Serverless SQL warehouse | 0.91 |
| Automated serverless, jobs | 0.50 |
| Jobs compute, classic | 0.30 plus VM |

**What it actually cost.** The whole build, five additional sources plus the silver and gold
rebuilds plus two full pipeline runs, came to about 74 SEK, under 8 USD. Two complete 14 task runs
at 280 seconds each barely registered. Storage is negligible at well under 1 GB.

**Cost controls that worked**

- Serverless jobs for the pipeline, serverless notebooks only while developing.
- Raw landed once, every fetch skips files that exist, so reruns re-parse instead of re-downloading.
- No Delta Live Tables and no SQL warehouse. Neither adds anything this project needs.
- ADF reused from Lab 1, and ADF bills per activity run with no idle cost.
- Everything in one region.

**Monitoring:** budget `slutprojekt`, 400 SEK per month, alerts at 50, 75 and 90 percent, plus an
`az` check of `currentSpend` after each working session.

## 9. Work plan

**Phase 0: preparation** (done 2026-09-23)
- [x] Catalog, schemas, external volume, budget, API keys in Key Vault

**Phase 1: bronze** (done)
- [x] SCB `TAB3277` and `TAB3276`, 2015 onward
- [x] Electricity prices, 12 months, 1,460 files
- [x] Seed table `bronze.kommun_elomrade`, 290 kommuner, 71 flagged uncertain
- [x] Cost check

**Phase 2: silver** (done)
- [x] Clean and type all sources, kommunkod as the key everywhere
- [x] Adult population denominator from single year ages
- [x] Quality checks, 30 across the two notebooks, all pass
- [x] Cost check

**Phase 3: gold** (done)
- [x] Dimensions `dim_kommun`, `dim_tid`, `dim_elomrade`
- [x] Facts `fact_elbilar`, `fact_laddinfra`, `fact_nyreg`, `fact_elpris`
- [x] `laddstolpe_ranking` with six components, weights as named profiles, sensitivity test
- [x] `utbyggnadsplan`, 50 stations awarded one at a time, two caps
- [x] Sanity check the top 20 against reality

**Phase 4: extra sources** (done)
- [x] Kolada: charge points, driving distance, median income
- [x] Trafikanalys: cars in traffic by fuel, which replaced the estimate
- [x] NOBIL: charge point counts per kommun, replacing OpenChargeMap
- [x] SCB `TAB628`: land area and density
- [x] SMHI: winter temperature
- [x] Trafikverket: tested and rejected on 19 of 290 coverage

**Phase 5: pipeline and documentation** (done)
- [x] Databricks job, 14 tasks, serverless
- [x] Run twice, nine tables IDENTISK
- [x] `documentation.md`

**Phase 5b: visualisation**
- [x] Kommun GeoJSON keyed on kommunkod
- [x] Sweden map, 7 data layers plus 3 station layers, standalone HTML
- [ ] Charts: top 20 by component, EV share over time, price per elområde, diminishing returns
- [ ] Decide the final format

**Phase 6: VG**
- [x] Split the Databricks job into four by layer: `sp_bronze`, `sp_silver`, `sp_gold`, `sp_allokering`
- [x] Grant the ADF managed identity workspace access and `CAN_MANAGE_RUN` on the four jobs
- [x] `pl_kor_databricks_jobb` and `pl_slutprojekt` in ADF, created through the REST API
- [x] `profil` parameter reaching the notebook from ADF; the five named presets it originally
      carried were later removed, superseded by the sliders in the web app
- [x] Monthly trigger `tr_manadsvis_5e`, the 5th at 03:00, Started
- [x] Retry 2 at 60 s, a Fail activity naming the layer, and `alert-pipeline-misslyckad`
- [x] `runbook.md`
- [x] Re-runnable through ADF: two full runs, all nine tables IDENTISK, after fixing a
      non-deterministic `approxQuantile` that the check caught
- [ ] Three screenshots with the username visible: silver, gold, rerun without duplicates
- [ ] Fifteen minute presentation, `presentation/presentation.html`

## 10. Visualisation

**The map** is a choropleth of the 290 kommuner. It began as `50_karta`, a folium notebook saving a
standalone HTML file, and that file is now retired: the web app serves the same map from Azure SQL
with the weight sliders beside it, so the picture and the model that produced it move together. The
notebook is kept as the record of how the geometry and the colour ramps were built.

Layers are toggled one at a time rather than stacked, which is how you see why a kommun ranks where
it does.

**Three decisions worth defending:**

- **No basemap.** CartoDB and Stadia require API keys and OpenStreetMap returns 403 to automated
  clients under its usage policy. The polygons draw Sweden by themselves, and removing tiles means
  the map cannot fail on venue wifi.
- **Yellow to red, never red to green.** Roughly one man in twelve cannot separate red and green.
- **Log scale for unmet need**, which runs from under a hundred to 440,265.

**Charts still to build**

| Chart | Point it makes |
|---|---|
| Top 20, stacked by component | why these win, and on which criterion |
| Plug-in share of new registrations per year | 2.4 percent in 2015 to 64 percent in 2026 |
| Average price per elområde | the four bands are real, and why price is capped at 0.5 |
| Diminishing returns from `utbyggnadsplan` | Tjörn drops 50 percent on one batch, Stockholm 1 percent |

## 11. Presentation, five minutes

| Minutes | Content |
|---|---|
| 0:45 | The question, and the answer: the twenty kommuner |
| 1:00 | Architecture, and why a star schema |
| 0:45 | Rerun without duplicates, the Delta version comparison |
| 1:00 | ADF orchestration, the schedule and its cost motivation |
| 0:45 | The allocation and the live map |
| 0:45 | What went wrong: the discarded estimate |

The last item is the strongest material available and should not be cut for time.

## 12. Risks and open questions

| Risk | Handling |
|---|---|
| Kommun to elområde is our own assumption | 71 kommuner flagged `elomrade_saker = false`, and the map has a layer for it |
| The weighting decides the winner | the weights are defined in one place, recorded with every answer, and adjustable live in the web app |
| Trafikanalys places a car at its owner's address | Solna shows 278.6 per 1,000 adults against Stockholm's 100.4. The owner split is not published for electric cars, so a sensitivity test is used instead: 9 of the top 10 survive |
| Home charging is not separated from fast charging | stated as a limitation. Closing it needs road traffic, which is parked with Lastkajen |
| 13 kommuner have no charger figure | national median with a flag, never zero. They hold 1.1 percent of the country's electric cars |
| Two implementations of the allocator, if the front end is ever built | parked, and the page would verify itself against the pipeline |
| Cost creep | measure after every phase, jobs rather than notebooks for repeated runs |

## 13. Conventions

- Notebooks numbered by layer: `0x_bronze_`, `1x_silver_`, `2x_gold_`, `3x` allocation, `4x` proof,
  `5x` presentation
- Tables in snake_case, Swedish domain terms kept (`kommunkod`, `drivmedel`, `elomrade`)
- Codes as text, never integers, so leading zeros survive
- Explicit encodings on every file read. SCB is ISO-8859-1, SMHI is UTF-8 with a BOM
- ANSI mode is on: `try_cast` and `try_divide` on any data we did not generate
- Every code cell has a short documentation cell above it, one to three lines
- All code lives in Databricks notebooks in the workspace, including SQL. The repo keeps planning
  and documentation
- ADF and Databricks job definitions are created through their REST APIs, not the portal, so they
  are reproducible
- `assignment.md` is kept in its original Swedish wording and is not edited
