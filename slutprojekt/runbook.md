# Runbook: slutprojekt charging station pipeline

How to run it, how to see whether it worked, and what to do when it does not. Written for someone who
did not build it.

| | |
|---|---|
| Orchestrator | Azure Data Factory `adf-molnkurs-dev-01`, resource group `rg-molnkurs-dev-01` |
| Compute | Azure Databricks `dbw-molnkurs-dev-01`, serverless job compute |
| Data | Unity Catalog `slutprojekt`, schemas `bronze`, `silver`, `gold` |
| Raw files | external volume `slutprojekt.bronze.raw` |
| Schedule | monthly, the 5th at 03:00 |
| Normal duration | about 9 minutes, of which bronze is about 4 |

## 1. What runs, in what order

```
ADF pl_slutprojekt
  kor_bronze     --> sp_bronze       9 tasks, 8 of them in parallel
  kor_silver     --> sp_silver       3 tasks
  kor_gold       --> sp_gold         2 tasks
  kor_allokering --> sp_allokering   1 task
  kor_export     --> sp_export       1 task
```

Each step calls `pl_kor_databricks_jobb`, which starts the Databricks job, polls every 30 seconds
until it is finished, then checks whether it finished **well**.

| Databricks job | Job id |
|---|---|
| `sp_bronze` | 1060855603268389 |
| `sp_silver` | 104273909942837 |
| `sp_gold` | 1117903459449932 |
| `sp_allokering` | 627114721457668 |
| `sp_export` | 841507927778801 |

## 2. How to run it

### Normal run

Portal, Data Factory Studio, Author, Pipelines, `pl_slutprojekt`, **Debug** for a one off, or
**Trigger now** for a real run. The only parameter is `profil`.

From the command line:

```bash
az datafactory pipeline create-run \
  --factory-name adf-molnkurs-dev-01 --resource-group rg-molnkurs-dev-01 \
  --name pl_slutprojekt --parameters '{"profil":"balanserad"}'
```

### Choosing a weighting

`profil` selects a named set of weights. It reaches the notebooks as a Databricks job parameter and
is written into `gold.laddstolpe_ranking._profil` and `gold.utbyggnadsplan._profil`, so any result
says which weighting produced it.

There is **one** weighting in the pipeline, `balanserad`. `profil` is still a parameter so that
every answer records which weighting produced it, but any other value now fails on purpose.

| Component | Weight |
|---|---|
| `behov` | 2.0 |
| `volym` | 1.0 |
| `tillvaxt` | 1.0 |
| `transit` | 1.0 |
| `elpris` | 0.5 |
| `kyla` | 0.5 |

Alternative weightings are explored in the web app, which recomputes the ranking and the whole
allocation in the browser without a pipeline run. That is why the five named profiles that used to
live in the notebook were removed: they solved a problem the app solves better.

The six components, in the order they appear in `_vikter`:

| Component | Measure | Scores high when | Because |
|---|---|---|---|
| `behov` | electric car kilometres per existing fast charge point | **more** km per charge point | demand already outruns supply |
| `volym` | electric cars, log scale | **more** cars | a station there serves more drivers |
| `tillvaxt` | change in electric cars per 1,000 adults over three years | **faster** growth | the fleet is arriving, build ahead of it |
| `transit` | traffic work on the road network, vehicle km per day, log scale | **more** traffic | travellers charge where they drive, not where they live |
| `elpris` | average price in the kommun's elområde | a **lower** price | cheaper power, so the site is cheaper to run |
| `kyla` | winter mean temperature at the nearest SMHI station | a **colder** winter | cold costs a battery about a quarter of its range, so the same journey needs more charging |

The weights are defined once, in `VIKTER` in `21_gold_ranking`. `30_allokering` reads `_profil`
and `_vikter` back out of the ranking table rather than keeping its own copy, and the web app reads
the same string from Azure SQL, so there is no second copy to drift. `30_allokering` does keep the
component names in `KOMPONENTER`, and that list must stay in the same order as `_vikter`, because
the two are zipped together positionally.

`transit` drops out of both the numerator and the denominator when `gold.fact_vagnat` has not been
loaded, so a run without road data still produces scores on the same 0 to 1 scale.

### Running one layer only

Useful when a single layer failed and the earlier ones are known good.

```bash
az datafactory pipeline create-run \
  --factory-name adf-molnkurs-dev-01 --resource-group rg-molnkurs-dev-01 \
  --name pl_kor_databricks_jobb \
  --parameters '{"job_id":"104273909942837","lager":"silver","profil":""}'
```

Leave `profil` empty for bronze and silver. Those jobs do not declare the parameter, and sending a
parameter a job does not know about is an error.

**Re-running a layer is safe.** Every layer overwrites from the layer below and nothing reads its own
output, so nothing accumulates. Bronze additionally skips any raw file that already exists, so a
re-run re-parses rather than re-downloading.

## 3. How to tell whether it worked

**In ADF:** Monitor, Pipeline runs. A green `pl_slutprojekt` means all five layers succeeded.

**In Databricks:** Workflows, Job runs, filtered to the five `sp_` jobs.

**In the data**, which is the check that actually matters:

```sql
-- the answer, and which weighting produced it
SELECT runda, kommun, stationer_i_rundan, _profil, _beraknad_at
FROM slutprojekt.gold.utbyggnadsplan ORDER BY runda;

-- did anything land twice
SELECT runda, COUNT(*) FROM slutprojekt.gold.utbyggnadsplan
GROUP BY runda HAVING COUNT(*) > 1;
```

**To prove a re-run changed nothing**, run the notebook `40_idempotens`. It compares every table's
current Delta version against the previous one with `EXCEPT ALL` in both directions and must report
IDENTISK for all of them. Audit columns such as `_beraknad_at` are excluded, because they are meant
to change.

## 4. When it fails

The Fail activity names the layer, the reason, the Databricks message and a direct link to the run.
Start there. The layer tells you which of the four sections below to read.

### The pipeline fails immediately on `starta_jobbet`

| Symptom | Cause | Fix |
|---|---|---|
| HTTP 403 | The ADF managed identity lost its Databricks access | Re-grant: the identity `adf-molnkurs-dev-01`, application id `8902e935-4350-4dba-9645-42062bb3969c`, needs the `workspace-access` entitlement and `CAN_MANAGE_RUN` on all five jobs |
| HTTP 400, unknown parameter | `profil` sent to a job that does not declare it | Only gold and allokering take `profil`. Send an empty string to bronze and silver |
| HTTP 404 | A job id changed, for example a job was recreated | Update the ids in `pl_slutprojekt` |

Note that the Databricks jobs **run as the workspace owner**, not as the ADF identity. That is why
ADF needs no Unity Catalog or storage permissions. If data access breaks, it is the owner's
permissions that changed, not ADF's.

### Bronze fails

Bronze talks to seven external APIs, so this is the layer that fails for reasons outside our control.

| Symptom | Cause | Fix |
|---|---|---|
| Timeout or HTTP 5xx from Kolada | Kolada returns 504 under load, seen during development | Re-run the layer. Each fetch already retries five times internally, and finished files are skipped |
| NOBIL returns nothing for any country code | The dump endpoint is sensitive to `countrycode` and the documentation contradicts itself | `05b_bronze_nobil` tries `SWE`, `SE`, `SVE` in order and prints which worked. If all three fail, check `nobil-api-key` in `kv-molnkurs-dev-01` and the current API docs |
| NOBIL rows fail the kommun join | NOBIL carries a few records mislabelled at source: Danish and Norwegian sites with `Land_code` swe | Expected, nine of them. Silver drops anything whose kommunkod is not Swedish. A jump above 50 fails the gate, which means the identifier scheme changed |
| SCB returns HTTP 429 | More than 30 calls in 10 seconds | The fetch honours `Retry-After` and paces itself. If it persists, SCB has tightened the limit, so raise `MIN_INTERVAL` |
| Trafikanalys returns 0 rows | The query collapsed instead of expanding. The region hierarchy only expands as `reglan|regkom` together | The notebook raises on this deliberately. Do not treat an empty file as success |
| A run takes far longer than usual | A raw file was deleted, so it is being downloaded again | Expected. It will be skipped next time |

### Silver or gold fails

These touch no APIs, so a failure here is almost always data shape or a quality check doing its job.

| Symptom | Cause | Fix |
|---|---|---|
| A check reports FAIL | A source changed shape or a national total moved | **Do not just update the expected number.** Find out what changed first. The checks exist because three separate bugs passed silently before they were added |
| `[DIVIDE_BY_ZERO]` or a cast error | ANSI mode is on in this workspace | Use `try_divide` and `try_cast` on anything we did not generate ourselves |
| `UNRESOLVED_COLUMN` after an edit | Spark matches column names case insensitively, so `withColumn("x")` overwrites `X` | Rename first, or use a name that differs by more than case |
| Row counts are a multiple of what they should be | A Kolada grain leaked in, either the 22 region codes or the 3,340 `G` sub areas | Filter `niva = 'kommun_eller_region'` and join to `silver.kommun` |
| Income or another Kolada figure jumps oddly | A gender split KPI has three values in no fixed order | Filter `kon = 'T'`. Never read `values[0]` |

### Allocation fails

| Symptom | Cause | Fix |
|---|---|---|
| `TypeError` mixing `Decimal` and `float` | A Spark `DECIMAL` column arrived in pandas as `decimal.Decimal` | Cast at `toPandas()`, which the notebook already does for the columns it uses. Add any new column to that list |
| "nothing eligible in round N" | Both caps blocked every kommun | Raise `max_per_kommun` or `max_per_lan` |
| The plan differs between runs | A tie broke differently | Should not happen: the frame is sorted by kommunkod and ties use a stable sort. If it does, that ordering was removed |

## 5. Things that are surprising but correct

- **Widgets do not reset.** `dbutils.widgets.text` leaves an existing widget's value alone, so
  changing a default in code does not change what your notebook uses. Run
  `dbutils.widgets.removeAll()` once. Job runs are unaffected: they use the declared defaults or the
  job parameters, never what your interactive widget shows.
- **`TERMINATED` does not mean success.** It is the only terminal state in Jobs API 2.2. Whether it
  went well is in `status.termination_details.code`. The ADF pipeline checks the second, not the first.
- **`skrivlage` is null in `DESCRIBE HISTORY`.** `saveAsTable` with overwrite on a managed table logs
  a replace, not a write, so there is no write mode to report. It does not mean nothing was written.
- **13 kommuner have no charger figure in Kolada** and are filled from NOBIL instead, flagged in
  `snabb_kalla`. Nine of them genuinely have **zero** fast charge points, which is correct and not a
  bug: Kolada never publishes a zero, its minimum is 1, so a missing row appears to mean "none".
- **A kommun with zero chargers scores very high** on unmet need, because the `+1` in the denominator
  is all that stops the ratio being infinite. Vaxholm has 653 electric cars and no fast charger.
  That is the model working, not a fault.
- **Raw data is landed once and skipped on a rerun**, and that is not a convenience, it is what makes
  the pipeline re-runnable and therefore what makes automatic retry safe. SMHI's station catalogue
  was the one fetch that broke the rule: it was read live every run, and SMHI returns it
  inconsistently, which made three gold tables differ between two runs of identical code. It is now
  landed as `raw/smhi/stationer.json` like everything else. If you ever need a fresh catalogue,
  delete that file and the next run fetches it again, keeping the best of three attempts.
- **`silver.temperatur` once matched every kommun to the wrong station.** A third of SMHI's rows have
  no coordinates and Spark sorts NULL first ascending, so the nearest-station race was won by a null
  distance. Fixed, and three gates guard it, including one asserting Norrland is colder than Skåne.
  A zero would mean infinite unmet need and would put them at the top of the ranking.
- **`arkiv/07_bronze_trafikverket` is archived, not in the pipeline.** It was tested and
  rejected, and it is kept as evidence. Do not add it to a job.

## 6. Cost

| | |
|---|---|
| Per run | about 9 minutes of serverless job compute |
| Per year on the schedule | twelve runs, on the order of 0.5 USD |
| Budget | `slutprojekt`, 400 SEK per month, alerts at 50, 75 and 90 percent |

```bash
az rest --method get --url "https://management.azure.com/subscriptions/<subscription-id>/providers/Microsoft.Consumption/budgets/slutprojekt?api-version=2023-05-01" \
  --query "properties.currentSpend" -o json
```

Read the currency label. Budgets report SEK at subscription scope while the credit is in USD.

**Why the schedule is monthly.** SCB publishes new registrations monthly, around the 2nd or 3rd, and
that is the fastest moving input that changes the answer. Trafikanalys and Kolada update yearly.
Electricity prices are daily but enter the model as a twelve month average. A daily trigger would
cost thirty times as much and produce identical numbers on twenty nine days out of thirty.
