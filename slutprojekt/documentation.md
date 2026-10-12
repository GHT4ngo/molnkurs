# Documentation: Slutprojekt, charging stations

Where 50 fast charging stations should go, and how the data platform behind that answer was built.
Screenshots are in `Screenshots/`.

| Resource | Name |
|---|---|
| Subscription | Azure for Students |
| Resource group | `rg-molnkurs-dev-02` |
| Region | Germany West Central |
| Databricks workspace | `dbw-molnkurs-dev-01` (Premium) |
| Storage account | `stmolnkurst4ngo02` (ADLS Gen2, container `unity`) |
| Access Connector | `ac-molnkurs-dev-01` |
| Key Vault | `kv-molnkurs-dev-01` |
| Unity Catalog | `slutprojekt`, schemas `bronze`, `silver`, `gold` |
| External volume | `slutprojekt.bronze.raw` |
| Job | `slutprojekt_pipeline`, 14 tasks, serverless |
| Budget | `slutprojekt`, 400 SEK per month |

---

## 1. The answer

The recommendation places the 50 stations in twenty kommuner, across all four price areas:

| Order | Kommun | Elområde | Electric cars | Fast charge points today | Car km per point | Stations |
|---|---|---|---|---|---|---|
| 1 | Solna | SE3 | 19,808 | 59 | 440,265 | 5 |
| 2 | Vaxholm | SE3 | 653 | 0 | 798,390 | 1 |
| 3 | Habo | SE3 | 608 | 0 | 714,439 | 1 |
| 4 | Nacka | SE3 | 11,609 | 43 | 338,065 | 3 |
| 5 | Stockholm | SE3 | 81,009 | 494 | 198,169 | 5 |
| 6 | Götene | SE3 | 430 | 0 | 481,382 | 1 |
| 7 | Vallentuna | SE3 | 1,596 | 4 | 381,981 | 1 |
| 8 | Svalöv | SE4 | 414 | 0 | 520,525 | 1 |
| 9 | Svenljunga | SE3 | 261 | 0 | 293,543 | 1 |
| 10 | Forshaga | SE3 | 238 | 0 | 289,597 | 1 |
| 11 | Göteborg | SE3 | 22,070 | 372 | 68,271 | 5 |
| 12 | Oxelösund | SE3 | 260 | 0 | 284,718 | 1 |
| 13 | Kungsbacka | SE3 | 5,660 | 63 | 108,871 | 5 |
| 14 | Luleå | SE1 | 2,581 | 62 | 44,977 | 5 |
| 15 | Svedala | SE4 | 909 | 4 | 208,637 | 1 |
| 16 | Tjörn | SE3 | 802 | 4 | 197,263 | 1 |
| 17 | Umeå | SE2 | 4,038 | 120 | 36,108 | 5 |
| 18 | Skellefteå | SE1 | 2,343 | 127 | 19,136 | 5 |
| 19 | Piteå | SE1 | 1,405 | 59 | 24,683 | 1 |
| 20 | Uppsala | SE3 | 7,759 | 166 | 54,174 | 1 |

Stations are awarded **one at a time** and unmet need is recomputed across all 290 kommuner after
each one, so the order column is the sequence the allocator chose, not a ranking. A kommun drops
down the list the moment it is served, which is why nine of the twenty receive a single station.

**Twenty was not chosen.** Two caps were: at most 5 stations per kommun and at most 15 per county.
Twenty is what the allocator made of them. The county cap binds in Stockholms län, which takes
exactly its 15 across Solna, Nacka, Vallentuna, Vaxholm and Stockholm.

**Nine of the twenty have no fast charger at all today.** Vaxholm has 653 electric cars and not one
fast charge point, which is 798,390 car kilometres per year per charge point, nearly twice Solna's
figure. That is the unmet-need component doing exactly what it was built for, and it only became
visible once the charge point figures were corrected: see section 7.

All four electricity price areas are represented: SE1 three times, SE2 once, SE4 twice, SE3 the rest.

## 2. Architecture

```
  APIs                     BRONZE                  SILVER                 GOLD
  ----                     ------                  ------                 ----
  SCB PxWebApi v2   --->   raw CSV in volume  ---> typed, kommunkod  ---> dim_kommun
  Trafikanalys      --->   raw JSON in volume      as the key             dim_tid
  Kolada v3         --->   one table per source    one table per          dim_elomrade
  Elpriset just nu  --->   as delivered            concept                fact_elbilar
  NOBIL             --->                           quality checks         fact_laddinfra
  SMHI              --->                                                  fact_nyreg
                                                                          fact_elpris
                                                                             |
                                                                             v
                                                                     laddstolpe_ranking
                                                                     utbyggnadsplan
```

Raw responses are written to the external volume `bronze.raw` before anything is parsed. The volume
is **external**, so dropping it would not delete the files. Every fetch skips a file that already
exists, which makes a rerun cost nothing and means the source APIs are called once.

| Notebook | Layer | Purpose |
|---|---|---|
| `00_setup` | | Catalog, schemas, external volume |
| `01_bronze_scb` | bronze | SCB `TAB3277` new registrations, `TAB3276` cars in traffic |
| `02_bronze_elpris` | bronze | Elpriset just nu, 12 months, 1,460 files |
| `03_bronze_elomrade` | bronze | Kommun to elområde seed table |
| `04_bronze_kolada` | bronze | Charge points, driving distance, median income |
| `04b_bronze_trafa` | bronze | Trafikanalys cars in traffic by fuel |
| `05b_bronze_nobil` | bronze | NOBIL charging sites and connectors |
| `06_bronze_scb_extra` | bronze | SCB `TAB628` land area and density |
| `arkiv/05_bronze_ocm` | | OpenChargeMap, superseded by NOBIL, see section 3 |
| `arkiv/07_bronze_trafikverket` | | Tested and rejected, see section 3 |
| `arkiv/50_karta` | | standalone map, superseded by the web app |
| `08_bronze_smhi` | bronze | Monthly mean temperature, 220 stations |
| `10_silver` | silver | The four original sources, plus the adult population |
| `11_silver_enrichment` | silver | The five added sources |
| `20_gold` | gold | Dimensions, facts, data quality evidence |
| `21_gold_ranking` | gold | The six components and the weights |
| `30_allokering` | gold | The 50 stations, one at a time |
| `40_idempotens` | | Proof that two runs produce identical data |

## 3. Data sources

| Source | Gives | Key | Grain |
|---|---|---|---|
| SCB `TAB3277` | new registrations by fuel | no | kommun, month, fuel |
| SCB `TAB3276` | cars in traffic by ownership | no | kommun, year |
| SCB `TAB638` | population by single year of age | no | kommun, year, age |
| SCB `TAB628` | land area and density | no | kommun, year |
| Elpriset just nu | electricity price | no | elområde, hour |
| Trafikanalys `t10026` | cars in traffic **by fuel** | no | kommun, year, fuel |
| Kolada v3 | charge points, driving distance, income | no | kommun, year |
| NOBIL | individual charging sites and connectors | yes | site |
| SMHI metobs | monthly mean temperature | no | station, month |
| Lastkajen, NVDB | annual average daily traffic | yes | road segment |
| Kommun boundaries, GeoJSON | kommun geometry and centre points | no | kommun |

**Ten statistical datasets from seven providers**, plus the kommun boundary GeoJSON, which is
reference geometry rather than a statistic but is fetched and landed like everything else. SCB
accounts for four of the ten through one API.

`TAB638` was fetched in Lab 2 and reused rather than fetched again.

**NOBIL replaced OpenChargeMap.** OpenChargeMap was the original source for charging sites and it is
the one the assignment suggests. It was dropped late in the project for two reasons. Its counts are
uneven, from 5 percent of the official figure in Karlstad to 107 percent in Varberg, and it claimed
26 fast charge points in Habo where there are none. NOBIL is the database Energimyndigheten reports
to and Kolada publishes from, so it is the source our primary figure is built on rather than a second
opinion: across the 277 kommuner where both exist the median ratio is **1.00**, and it covers all 290.
The OpenChargeMap notebook and tables are kept in the archive described in section 4.

### Trafikverket, tested and rejected

Traffic volume was planned as a source, on the argument that a fast charger on a motorway serves
drivers who do not live in the kommun. Trafikverket publishes `TrafficFlow`, which turns out to be
live readings from detector sites rather than annual average daily traffic, and the detectors sit in
three metro areas: **19 kommuner of 290**, with 327 sites in Salem and 2 in Nynäshamn.

Used as a ranking component it would have behaved as a hidden "Stockholm suburb" flag rather than a
measure of traffic. The notebook is kept as evidence. Annual traffic per road segment exists in NVDB
via Lastkajen, which needs a separate account and was out of scope.

## 4. Bronze

One table per source, stored as delivered, with `_source`, `_source_file` and `_ingested_at`.

**Retired objects live in `slutprojekt.arkiv`.** Work that was done and then superseded is not
deleted, because the reasoning is part of the hand-in, but it must not sit in bronze, silver or gold
where a reader would assume it is live. The archive schema holds five tables, each with a comment
saying why: the two OpenChargeMap tables, the rejected Trafikverket table, the OpenChargeMap-derived
`laddstationer`, and `fact_fordon`, which was removed once SCB and Trafikanalys turned out to report
the same figure. The three retired notebooks sit in `Slutprojekt/arkiv/` and their raw files in
`/Volumes/slutprojekt/bronze/raw/_arkiv/`. Everything in bronze, silver and gold is written and read
by something live.

Two things in this layer took real work.

**SCB sends ISO-8859-1**, not UTF-8, and the encoding has to be set on every read.

**Reading a folder of CSV files with `header=True` is unsafe.** Spark takes the column names from
the first file and maps every other file by position. Twelve yearly files all landed under the 2015
column names, which produced 27,840 duplicate rows and a plug-in share of 0 percent. The fix is to
read each file on its own and unpivot it to long format before the union, so each file keeps its own
header.

## 5. Silver

Typed, cleaned, kommunkod as the key everywhere, and only rows that mean what we think they mean.
Every table is rebuilt from bronze with `overwrite`.

| Table | Grain |
|---|---|
| `silver.kommun` | kommun |
| `silver.nyreg` | kommun, month, fuel |
| `silver.fordon` | kommun, year |
| `silver.elpris_dag` | elområde, day |
| `silver.befolkning` | kommun, year |
| `silver.elbilar` | kommun, year |
| `silver.laddinfra` | kommun, year |
| `silver.kommun_fakta` | kommun |
| `silver.agarkategori` | kommun, year |
| `silver.nobil_laddinfra` | kommun |
| `silver.temperatur` | kommun |

Forty quality checks run across the three silver notebooks, each comparing against a figure worked
out from the source independently rather than read back out of the table being tested. All pass.

**117 checks run across the pipeline in total**, 42 in bronze, 40 in silver and 35 in gold and the
allocation. Every one of the fourteen notebooks ends with a gate: if any check fails the notebook
raises, the Databricks task fails, and Data Factory stops the run with a message naming the layer.
A check that only prints is not a check.

## 6. Data model

```
        dim_kommun                  dim_tid              dim_elomrade
   kommunkod, kommun, lan       datum, ar, manad,     elomrade, namn,
   elomrade, invanare,          kvartal, ar_manad     beskrivning
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

**Why a star schema and not one wide table.** The sources have three different grains: kommun and
year, kommun and month, elområde and day. A wide table forces the finest grain on all of them and
repeats every kommun name and coordinate on every row. Dimensions also mean that a new source
becomes a new fact table instead of a rebuild.

**Why the facts carry a `datum`.** Each fact stores the first day of its period, 1 January for a
yearly row and the 1st for a monthly one, so every fact joins `dim_tid` on a single column despite
having different grains.

**Why there is no `fact_fordon`.** SCB `TAB3276` and Trafikanalys `t10026` both report 5,029,589
cars in traffic for 2025, to the car, because SCB republishes Trafikanalys. Two tables claiming to
be the source of one number is worse than one. The agreement is recorded here and used as a
validation instead.

## 7. The ranking and its reasoning

| Component | Measure | Scores high when | Because |
|---|---|---|---|
| `otackt_behov` | electric car kilometres per existing fast charge point | **more** km per charge point | demand already outruns supply |
| `volym` | electric cars, log scale | **more** cars | a station there serves more drivers |
| `tillvaxt` | change in electric cars per 1,000 adults over three years | **faster** growth | the fleet is arriving, build ahead of it |
| `transit` | traffic work on the road network, vehicle km per day, log scale | **more** traffic | travellers charge where they drive, not where they live |
| `elpris` | average price in the kommun's elområde | a **lower** price | cheaper power, so the site is cheaper to run |
| `kyla` | winter mean temperature at the nearest SMHI station | a **colder** winter | cold costs a battery about a quarter of its range, so the same journey needs more charging |

| Choice | Reasoning |
|---|---|
| **Unmet need, not raw demand** | A kommun with many cars and many chargers is already served. The question is where demand outruns supply, so the first component is a ratio. |
| **Battery electric only** | Most plug-in hybrids cannot DC fast charge, so counting them would place demand where a fast station goes unused. Plug-in hybrids are carried as context. |
| **Kilometres, not cars** | A car driven 1,400 mil a year needs more charging than one driven 800. Kolada measures this per kommun, so it costs nothing to use. |
| **Volume on a log scale** | Counts run from 17 to 81,009. On a linear scale Stockholm takes the whole range and everything else rounds to zero. |
| **Growth per 1,000 adults, not a percentage** | A kommun going from 2 cars to 6 grew 200 percent and would outrank Göteborg. Absolute change per head does not explode on small bases. |
| **Adults as the denominator, 18 and up** | Cars belong to adults, so dividing by total population penalises kommuner with many children. Driving licences would be better and are published nationally only: 6,620,587 class B licences against 8,431,845 adults, so 78.5 percent of adults hold one. Cutting the band at 65 would have discarded a third of them. |
| **Min-max normalisation** | It preserves magnitude, so twice the measure scores twice as high. Percentile rank is more robust to outliers but throws that away. |
| **Missing charger figures come from the source upstream, not from an average** | Kolada reports a charge point count for 277 of 290 kommuner. The thirteen were originally given the national median, 26. That was wrong in a way that changed the answer: Vallentuna was credited with 26 when it has 4, which put it 47th instead of near the top. They are now filled from NOBIL, the database Kolada itself publishes from, used as it comes with no scaling factor because the two agree at a median ratio of 1.00 across the 277 where both exist. A `snabb_kalla` column records whether each number came from Kolada or NOBIL. |
| **A measured zero is kept as zero** | Nine of the thirteen genuinely have no fast charge point. Kolada never publishes a zero, its minimum across 277 kommuner is 1, so a missing row appears to have meant "none" all along. We do not substitute an average for a measured zero: `+1` in the denominator stops the ratio being infinite, and that constant is what separates a kommun with no charger from one with a single charger. It is a smoothing choice, stated rather than hidden. |
| **The weighting is exposed, not hidden** | It is a judgement, not a fact, so it is defined in one place, recorded with every answer, and adjustable live in the web app without re-running anything. |

**One weighting, and every number in it has a reason.**

| Component | Weight | Why |
|---|---|---|
| `behov` | 2.0 | the question being asked: where demand outruns supply |
| `volym` | 1.0 | 50 physical stations serve absolute numbers of cars |
| `tillvaxt` | 1.0 | the fleet is still growing quickly |
| `transit` | 1.0 | a charger serves travellers as well as residents |
| `elpris` | 0.5 | only four distinct values nationally |
| `kyla` | 0.5 | close to a latitude proxy, and read from a distant station |

The notebook previously held five named profiles. They were removed once the web app gained
sliders: trying a different weighting is now a second in the browser rather than a pipeline run, so
the pipeline keeps only the weighting it records with the answer. `profil` remains an ADF parameter
so that every result still states which weighting produced it, and any value other than
`balanserad` now fails with a message pointing at the app.

**A limitation of the price component.** Electricity price is published per elområde, so across 290
kommuner it takes exactly four values. It is a zone label rather than a measurement that varies by
kommun, and at equal weight the gap between the north and SE3 alone contributes 0.16 to the total
score, more than the combined spread of the other three components across most of the country. Its
weight is therefore set to 0.5 rather than 1, and a quality check asserts that the component has
only four distinct values so the limitation stays visible.

**A limitation of the cold component.** Cold is a real effect: a battery loses roughly a quarter of its
range in winter, so the same kilometre driven in Norrbotten needs more charging than one driven in
Skåne. The measure is `vintermedel_c` from SMHI, inverted so the coldest kommun scores 1. Two things
keep its weight at 0.5. Winter temperature in Sweden is close to a function of latitude, so at a higher
weight the component stops measuring anything and becomes a north versus south switch, which is the
same objection as for price. And the figure is borrowed from the nearest weather station, which
`silver.temperatur` records as `stationsavstand_km` and which can be several tens of kilometres away,
so it is a regional value rather than a kommun measurement.

At 0.5 the component does visible but bounded work: Luleå rises from rank 9 to rank 5 in the balanced
plan and Skellefteå displaces Uppsala in tenth place, while the other eight kommuner are unchanged.

## 8. How the 50 stations are shared out

A ranking says which kommun is most attractive. It does not say where 50 stations go, because the
moment you build in the top kommun it stops being the top kommun. Stations are therefore awarded
**one at a time**, and unmet need is recalculated across all 290 kommuner after every single one.

The batch size is a parameter and it used to be 5. At 5 the answer could only arrive in blocks of
five and it landed on ten kommuner. At 1 the recalculation bites after every station, so a small
kommun can take one and immediately fall behind: Svedala and Tjörn do exactly that, and the answer
spreads over twenty kommuner. It costs nothing, 50 passes over a 290 row frame.

The notebook previously produced three further plans alongside the balanced one, `akut`, `tillvaxt`
and `transit`, each scoring on a single component with the rest at zero. The argument was that a
balanced weighting hides the choice between "fix it now" and "build for later", and that management
should make that choice rather than inherit it. The argument still holds, but the web app answers it
better: any component can be dragged to zero and the resulting plan is recomputed in the browser
immediately. Precomputing three fixed versions only added rows nobody read, so they were removed.

The `strategi` and `strategivikter` columns remain on `gold.utbyggnadsplan`. With one plan they are
provenance rather than a key, recording what produced the rows exactly as `_profil` and `_vikter` do
in the ranking.

**Two constraints, both business rules rather than statistics**, and both parameters: at most 5
stations per kommun, and at most 15 per län. Without the second, all 50 landed in Stockholm county.
The model was not wrong, that is where the electric cars are, but a national programme that lands
entirely in one county is not what the assignment asks for. Stating it as a constraint is honest;
reweighting the model until the map looks better would not be.

**On diminishing returns.** The first batch buys the largest reduction and each one after it buys
less, but the effect is small: 50 stations against 12,052 existing fast charge points is 0.4 percent
of national supply, so Stockholm moves 1 percent after ten stations. The clearest case is Tjörn,
where 802 electric cars share 4 fast charge points and five stations halve the ratio. The
constraints, not the feedback, are what spread the programme out.

## 9. Pipeline and re-runnability

`slutprojekt_pipeline`, 14 tasks on serverless job compute. The eight bronze tasks run in parallel,
then silver, then gold, then the allocation.

```
setup
  |-- bronze_scb ------------+
  |-- bronze_elpris ---------+--> silver --> silver_enrichment --> gold --> gold_ranking --> allokering
  |-- bronze_elomrade -------+                      ^
  |-- bronze_kolada ---------------------------------|
  |-- bronze_trafa ----------------------------------|
  |-- bronze_nobil ----------------------------------|
  |-- bronze_scb_extra ------------------------------|
  |-- bronze_smhi -----------------------------------+
```

**Serverless job compute, not notebook compute.** Jobs bill at 0.50 USD per DBU hour against 1.00
for interactive notebooks, so the finished pipeline runs at half the rate it was developed at.

**Idempotency is by overwrite, not by merge.** Every layer rebuilds from the layer below, and
nothing reads its own output, so a rerun cannot accumulate. `MERGE INTO` would be the tool if bronze
arrived incrementally; it does not.

The greedy allocator is the one place where a rerun could legitimately differ, because a tie between
two kommuner could break either way. The frame is sorted by kommunkod before the loop and ties break
with a stable sort, so the plan is deterministic.

**The evidence.** Two runs completed in 282 and 279 seconds. `40_idempotens` then compares each
table's current Delta version against its previous one with `EXCEPT ALL` in both directions,
excluding the audit columns whose job is to change:

| Table | Rows | Differences |
|---|---|---|
| `gold.utbyggnadsplan` | 30 | 0 |
| `gold.laddstolpe_ranking` | 290 | 0 |
| `gold.dim_kommun` | 290 | 0 |
| `gold.fact_elbilar` | 3,190 | 0 |
| `gold.fact_laddinfra` | 870 | 0 |
| `gold.datakvalitet_estimat` | 290 | 0 |
| `silver.nyreg` | 324,800 | 0 |
| `silver.elbilar` | 3,190 | 0 |
| `silver.laddinfra` | 870 | 0 |

A separate check confirms no duplicates on the natural key of each table, because "identical between
runs" is not the same as "correct": a pipeline that appended rather than overwrote could be stably
wrong and still match itself.

### 9b. Orchestration with Azure Data Factory

Running notebooks by hand is not a pipeline. ADF owns the ordering, the schedule and the error
handling, while Databricks does the work.

`adf-molnkurs-dev-01` is reused from Lab 1, so there is no new resource and no idle cost: ADF bills
per activity run. The Databricks work is split into four jobs by layer, `sp_bronze`, `sp_silver`,
`sp_gold` and `sp_allokering`, and ADF sequences them.

```
pl_slutprojekt:   bronze --> silver --> gold --> allokering
                    each one through pl_kor_databricks_jobb
                    (Web run-now --> Until poll --> If not SUCCESS --> Fail)
```

Two pipelines rather than one, so the polling logic exists once and each layer is a parameterised call.

**ADF drives Databricks through the Jobs REST API, not the native Databricks Notebook activity.** The
native activity needs a cluster spec, and every option is classic compute. A new job cluster is
created **per activity**, each taking three to five minutes to start and billed for it, so fourteen
activities would spend roughly an hour on startup for a five minute workload. A Web activity calling
`jobs/run-now` keeps everything on serverless job compute, which starts in seconds at half the DBU
rate.

Authentication is the ADF managed identity against the Databricks resource id. The identity has to be
added to the workspace and granted **Can Manage Run** on the jobs: being able to create a Data Factory
is a control plane permission and says nothing about being allowed to run a job inside Databricks.
That distinction has now appeared four times in this course.

### 9c. Schedule, and why monthly

**Monthly, the 5th at 03:00.**

| Source | Updates |
|---|---|
| SCB `TAB3277` new registrations | monthly, published around the 2nd or 3rd |
| Trafikanalys, Kolada | yearly |
| Elpriset just nu | daily, but enters the model as a 12 month average |

The schedule is set by the fastest moving input that actually changes the answer, which is the monthly
registration release, and the 5th leaves SCB a couple of days of margin.

On cost: twelve runs a year at about five minutes of serverless job compute is on the order of **0.5
USD per year**. A daily trigger would cost thirty times as much and produce identical numbers on
twenty nine days out of thirty, because the underlying sources have not moved.

### 9d. Error handling

Two layers, because API failures and task failures are different problems.

**Inside each notebook**, every fetch retries on a non-200 response with a pause between attempts, up
to five times, and honours `Retry-After` on a 429. This handles the transient case, which is the common
one: Kolada returned HTTP 504 under load during development.

**In ADF**, the Web activity retries twice at 60 second intervals. If the layer still fails, a **Fail**
activity stops the pipeline with a message naming the layer, the run id and the Databricks run URL, so
the error says where to look rather than only that something broke. An Azure Monitor alert on failed
pipeline runs sends an email.

Retrying a whole layer is only safe because the pipeline is idempotent and the fetches are resumable:
a retried bronze run skips every file already on disk and re-parses rather than re-downloading. The
re-runnability built for the assignment is what makes automated retry sane rather than risky.

Troubleshooting and the known failure modes are in `runbook.md`.

## 10. The estimate that was discarded

The first version of this model estimated the electric car stock per kommun by cumulating new
registrations from 2015, because SCB has no kommun level table of the fleet by fuel. The result did
not survive contact with reality.

| Kommun | Estimated | Actual | Ratio |
|---|---|---|---|
| Sundbyberg | 9,612 | 2,473 | 3.89 |
| Lund | 24,540 | 6,617 | 3.71 |
| Mölndal | 19,311 | 8,451 | 2.29 |
| Malmö | 50,550 | 22,860 | 2.21 |
| Stockholm | 265,109 | 138,763 | 1.91 |

Cars are first registered where the dealer, the leasing company or the corporate head office sits,
and are then driven somewhere else. The **median kommun has a ratio of 0.97**, so the estimate was
nearly right for a typical kommun and wrong by up to a factor of four for about 15 of them,
including Stockholm, Malmö and Lund. That is enough to make it useless for ranking.

Company ownership explains only part of it. The correlation between the ratio and the share of
company owned cars is **0.10**, and the two worst cases have below average company shares. The
outliers do tilt that way, a median of 21.4 percent against a national 17.2, but that is a tilt and
not a cause.

The fix was to replace the derived metric with a measured one: Trafikanalys `t10026`, which
publishes cars in traffic per kommun and fuel. The estimate is kept in
`gold.datakvalitet_estimat` rather than deleted, because it is the evidence behind the largest
decision in the project.

## 10b. Five bugs the checks caught

Every one of these produced a plausible looking table. None would have been found by counting rows.

| Bug | How it looked | How it was caught |
|---|---|---|
| **An estimate four times too large** | Electric cars per kommun, derived by cumulating new registrations. Sundbyberg 9,612 against a real 2,473. | Comparison against Trafikanalys, an independent source. Every check that only looked at our own data passed. |
| **A non-deterministic median** | `approxQuantile` with a relative error of 0.01 is partition dependent, so two identical runs produced different medians. Because that value set the top of the scale, all 290 rows moved. | The re-runnability proof, comparing values rather than counts. Row counts were right and there were no duplicates. |
| **An average hiding a candidate** | Thirteen kommuner with no charge point figure were given the national median, 26. Vallentuna has 4. | Checking the placeholder against a second source instead of accepting it. |
| **A join that never ran** | Winter temperature per kommun. A third of SMHI's rows carry no coordinates, and Spark sorts NULL first on an ascending order, so `row_number()` handed every kommun a station whose distance was never computed. Kiruna came out at +2.38 C, Östersund at -11.74 C. | Looking at the map. The table was full, every kommun had a value, and `stationsavstand_km` was NULL for all 290 in plain sight. |
| **A fetch that was never landed** | Every source in the project lands its raw data once and skips it on a rerun. One did not: SMHI's station catalogue, which carries the coordinates, was fetched live on every run. SMHI returns the temperatures identically but the catalogue inconsistently, 175 then 177 of 230 stations with a position. Piteå's nearest station moved 25.0 km to 32.6 km, its winter mean moved 2.9 degrees, and three gold tables differed between two runs of identical code. | `40_idempotens`, comparing two consecutive runs. Nothing else could have found it: both runs were internally consistent and every other check passed. |

The pattern is the same in all five: **a check that compares the data against itself will pass while
being completely wrong.** The checks that caught these compare against an independent source, a
previous version, or a physical expectation. The last one is now a gate: whatever the join does,
Norrland must come out colder than Skåne.

## 11. Cost analysis

| | |
|---|---|
| Credit | 100 USD, Azure for Students, expires 2027-08-20 |
| Spent, 7 Sep to 6 Oct 2026 | **541.93 SEK** |
| Of which Azure Databricks | **518.22 SEK**, 96 percent |
| Data Factory, monitoring, storage, gateway | 23.71 SEK together |
| Forecast for the period | 590.08 SEK |
| Budget | `slutprojekt`, 400 SEK per month, alerts at 50, 75 and 90 percent |

**Ninety-six percent of the bill is compute.** Orchestrating the pipeline, storing every raw file,
serving the answer from Azure SQL and monitoring the whole thing cost under 24 kronor between them.
A full pipeline run is about one krona. An open notebook is about twenty kronor an hour whether or
not anybody is typing, which is where the money actually went: the most expensive mistake in the
project was a polling loop that waited on a slow external order while the cluster billed, and two
hours of that cost more than forty pipeline runs. The fix was to stop waiting in a notebook and
watch the supplier's own web page instead.

The 400 SEK monthly budget is exceeded and the period is forecast to reach 590 SEK. That is the
budget alert doing its job, and it is the reason the trigger is monthly rather than daily.

Read the currency label before reading the number: budgets report SEK at subscription scope while
the credit is denominated in USD.

| Measure | Effect |
|---|---|
| Serverless **jobs** for the finished pipeline | 0.50 USD per DBU hour instead of 1.00 |
| Raw files landed once, fetches skip existing files | Two full pipeline runs cost almost nothing |
| No SQL warehouse, no Delta Live Tables | Both would add cost without adding anything the project needs |
| Everything in one region | No cross region traffic |
| Spending limit **On** | Resources stop rather than generating a bill |

Databricks prices used, Premium, Germany West Central: interactive serverless 1.00, serverless jobs
0.50, serverless SQL warehouse 0.91 USD per DBU hour. Storage is negligible: the whole project is
far below 1 GB at 0.018 USD per GB per month.

The entire build, five additional sources plus the silver and gold rebuilds plus two pipeline runs,
cost about 74 SEK, under 8 USD.

## 11b. Security

**No secret exists in the repository or in any notebook.** Four credentials live in Key Vault
`kv-molnkurs-dev-01`, surfaced to Databricks through the Key Vault backed secret scope `slutprojekt`
and read with `dbutils.secrets.get`, whose output Databricks redacts. That is why the notebooks are
safe to screenshot.

| Identity | May | May not |
|---|---|---|
| Access Connector `ac-molnkurs-dev-01` | read and write the `unity` container | anything else |
| ADF managed identity | start the five Databricks jobs and read their status | edit or delete them |
| App Service managed identity | `SELECT` on schema `gold` in Azure SQL | write, or read any other schema |
| `databricks_writer` | write to schema `gold` | touch the rest of the database |

**The web app has no password at all.** It authenticates to Azure SQL with its managed identity, so
there is no connection string to leak, rotate or commit by accident.

### What is deliberately not protected

Stating this is part of the answer, not an admission.

- **The web app is public.** Anyone with the link can open it. Accepted: every figure on it is
  already published by SCB, Kolada, Trafikanalys and NOBIL.
- **The SQL firewall allows all Azure services**, not only ours. The correct fix is a private
  endpoint, which needs a paid tier. Accepted for a student project.
- **One password still exists**, `sqldb-writer-password`, because Databricks writes to Azure SQL as
  a contained user rather than with a managed identity. It lives in Key Vault and never appears in
  code, but it is a password, and it is the weakest link we kept.

### Two keys removed, and one blast radius closed

`ocm-api-key` and `trv-api-key` were deleted from Key Vault once their sources were retired, leaving
four secrets. Key Vault soft delete keeps them recoverable for 90 days.

More seriously, `alert-pipeline-misslyckad` was wired to `ag-molnkurs-dev-01`, which is **Lab 1's**
action group: it is used by the `vg_kreditvakten` budget and its Logic App receiver does nothing but
`POST .../sites/app-molnkurs-t4ngo-01/stop`. A failed Slutprojekt pipeline would therefore have shut
down Lab 1's web application. The alert now has its own action group, `ag-slutprojekt-dev-01`, with a
single email receiver. Reusing an action group is reusing its blast radius.

**The lesson the course kept teaching:** owning a resource is not permission to read its data. That
appeared four separate times, on blob storage, on Unity Catalog storage, on Key Vault and on a newly
created second vault. The control plane role and the data plane role are different things.

## 12. Limitations, stated plainly

| Limitation | Why it matters |
|---|---|
| **Home charging is not separated from fast charging** | Dense urban kommuner score high because they have many cars per charge point, but apartment dwellers mostly need slow overnight charging. Fast charging is a travel corridor problem, and the source that would have answered it covers 19 of 290 kommuner. |
| **Trafikanalys places a car at its owner's address** | Solna shows 278.6 electric cars per 1,000 adults against Stockholm's 100.4, because leasing fleets are registered there. The owner type split is not published for electric cars at any level, so it cannot be removed. A sensitivity test that rescales by private ownership share keeps 9 of the top 10 with a largest move of 3 places. |
| **The kommun the model is most confident about is Solna** | Which is also the least trustworthy figure in the model. |
| **Elområde for 71 kommuner is our own assumption** | Six counties are split between price areas and no public API gives the mapping, so a documented latitude rule was used. Those rows carry `elomrade_saker = false`. |
| **13 kommuner have no official charger figure** | They hold 1.1 percent of the country's electric cars. Their supply comes from NOBIL rather than Kolada and is flagged in `snabb_kalla`. Four of them reach the build plan, so the substitution is load bearing: if NOBIL and Kolada ever stop agreeing, a gate in `21_gold_ranking` fails the run rather than letting the fill drift. |
| **The cold component was wrong until 2026-10-06** | Winter temperature was matched to the nearest SMHI station, but a third of SMHI's rows carry no coordinates and Spark sorts NULL first on an ascending order, so every kommun was handed a station whose distance was never computed. Kiruna was given +2.38 C and Östersund -11.74 C. It is fixed and three gates now guard it, including one asserting that Norrland comes out colder than Skåne. See section 10b. |
| **NOBIL is a live database, Kolada is an annual snapshot** | The two agree at a median ratio of 1.00 today, which is what makes the fill for 13 kommuner safe. NOBIL moves daily, so a gate fails the run if the ratio ever leaves 0.75 to 1.35 rather than letting the two drift apart unnoticed. |
| **Charging site to kommun is provisional** | Sites are assigned to the nearest kommun centre, which is wrong for large or irregular kommuner. A point in polygon join replaces it when the map is built. |

## 13. Screenshots

**Required for hand in, three per student with the username visible in the Databricks top bar:**

| # | What it must show |
|---|---|
| 1 | A query against the **silver** layer, for example `silver.elbilar` joined to `silver.kommun` |
| 2 | A query against the **gold** layer, `gold.laddstolpe_ranking` top 20 or `gold.utbyggnadsplan` |
| 3 | A **rerun without duplicates**: the `40_idempotens` version comparison, all rows IDENTISK |

**Useful for the presentation:**

| File | What to capture |
|---|---|
| `slutprojekt_adf_dag.png` | The ADF pipeline showing the four layers in sequence |
| `slutprojekt_adf_runs.png` | ADF monitor with a successful run and its duration |
| `slutprojekt_trigger.png` | The monthly trigger definition |
| `slutprojekt_catalog.png` | Unity Catalog with `slutprojekt` and its three schemas |
| `slutprojekt_volume.png` | The external volume and the raw files |
| `slutprojekt_job_dag.png` | The job graph, showing bronze running in parallel |
| `slutprojekt_job_runs.png` | Two successful runs with their durations |
| `slutprojekt_idempotens.png` | The version comparison, all rows IDENTISK |
| `slutprojekt_checks_silver.png` | The silver quality checks |
| `slutprojekt_ranking.png` | The top 20 of `laddstolpe_ranking` |
| `slutprojekt_utbyggnadsplan.png` | The allocation, in the order the stations were awarded |
| `slutprojekt_datakvalitet.png` | The discarded estimate against reality |
| `slutprojekt_budget.png` | The budget with current spend |
| `slutprojekt_karta.png` | The map in the web app, with the weight sliders visible |
