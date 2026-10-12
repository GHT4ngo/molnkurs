# Molnkurs: data platforms and cloud solutions

Course work from *Dataplattformar och molnlösningar*, part of the Data Engineering programme (DE25)
at Nackademin. Everything runs in Azure on a student subscription.

The main piece is the final project: a data warehouse in Azure Databricks that turns ten public
datasets into one answer to the question **where should 50 fast charging stations go?**

## Links

| What | Where |
|---|---|
| Live web app | [app-molnkurs-t4ngo-02.azurewebsites.net](https://app-molnkurs-t4ngo-02.azurewebsites.net) |
| Final project, full documentation | [slutprojekt/documentation.md](slutprojekt/documentation.md) |
| Final project, plan and decisions | [slutprojekt/project_plan.md](slutprojekt/project_plan.md) |
| Final project, runbook | [slutprojekt/runbook.md](slutprojekt/runbook.md) |
| Lab 1, ETL with Azure Data Factory | [labb1/documentation.md](labb1/documentation.md) |
| Portfolio | [t4ngo.com](https://t4ngo.com) |

The web app's database pauses itself after an hour without visitors, so the first load can take
about half a minute.

## Final project: charging stations

Management wants to place 50 fast charging stations where they do the most good. The deliverable is
a ranked build plan with the reasoning behind it, and a web page where you can change the
priorities and watch the answer move.

| | |
|---|---|
| 10 | open datasets, from seven providers |
| 290 | kommuner, every one scored |
| 117 | automatic checks that stop a bad run |

### The answer

Twenty kommuner share the fifty stations. The order is the sequence they were picked in, not a
ranking.

| # | Kommun | Stations | # | Kommun | Stations |
|---|---|---|---|---|---|
| 1 | Solna | 5 | 11 | Göteborg | 5 |
| 2 | Vaxholm | 1 | 12 | Oxelösund | 1 |
| 3 | Habo | 1 | 13 | Kungsbacka | 5 |
| 4 | Nacka | 3 | 14 | Luleå | 5 |
| 5 | Stockholm | 5 | 15 | Svedala | 1 |
| 6 | Götene | 1 | 16 | Tjörn | 1 |
| 7 | Vallentuna | 1 | 17 | Umeå | 5 |
| 8 | Svalöv | 1 | 18 | Skellefteå | 5 |
| 9 | Svenljunga | 1 | 19 | Piteå | 1 |
| 10 | Forshaga | 1 | 20 | Uppsala | 1 |

Vaxholm and Habo have 653 and 608 electric cars and no fast charger at all. Nine of the twenty are
in that position. All four electricity price areas are represented.

### How it is built

Three layers, one direction. Data comes in exactly as the source sent it, gets cleaned and checked,
then gets reshaped into the form the question needs. Nothing flows backwards, so any layer can be
rebuilt from the one before it.

| Layer | What it holds | Checks |
|---|---|---|
| Bronze | data as delivered, from 9 notebooks running in parallel | 42 |
| Silver | 12 typed and checked tables, keyed on kommunkod | 40 |
| Gold | 3 dimensions, 5 facts, the ranking and the build plan | 35 |
| Serving | 330 rows in Azure SQL, read by the web app | |

One failed check stops the whole run. Azure Data Factory owns the order between the layers, and
Databricks owns the parallel work inside one layer.

Sources: SCB (four tables), Trafikanalys, Kolada, Elpriset just nu, SMHI, NOBIL and Trafikverket
Lastkajen (NVDB road traffic).

### How a kommun is scored

Six measurements. Each is rescaled so the best kommun in the country scores 1 and the worst 0, then
weighted and averaged.

| Component | Weight | Why |
|---|---|---|
| Unmet need | 2.0 | this is the question: where does demand outrun supply |
| Volume | 1.0 | 50 real stations serve absolute numbers of cars |
| Growth | 1.0 | the fleet is still growing, so build where it grows |
| Through traffic | 1.0 | a charger serves travellers, not only residents |
| Electricity price | 0.5 | cheaper scores higher, but there are only four values in the country |
| Cold winter | 0.5 | cold costs a battery about a quarter of its range |

Price and cold are halved on purpose. At full weight they stop measuring a kommun and start
measuring north against south.

### How the fifty are shared out

Stations are awarded one at a time. After every station the chosen kommun has more supply than
before, so need is recomputed for all 290 before the next pick. Two caps apply: no kommun may hold
more than 5 stations and no county more than 15. Both are decisions rather than measurements, so
they are controls in the web app and not constants in the code.

### Azure services

| Service | What it does here |
|---|---|
| Databricks | runs all the code and holds the warehouse, on serverless jobs |
| Unity Catalog | names and permissions for every table |
| ADLS Gen2 storage | holds the raw downloaded files, so a rebuild never downloads again |
| Data Factory | runs the layers in order on a monthly schedule |
| Key Vault | stores the four API credentials, so none appears in code |
| Azure SQL serverless | a small copy of the answer for the web page |
| App Service Free | hosts the web page |
| Azure Monitor | sends an email if a pipeline run fails |

Data Factory starts each Databricks job through the Jobs REST API and polls until it finishes. The
built in Databricks activity would start a classic cluster for every step, three to five minutes
each and all of it billed. A failed job still reports `TERMINATED`, so the pipeline reads the
termination code and not the status.

### Running it twice

Run the pipeline again and you get 290 rows, not 580. Every layer is rebuilt from the layer below
and no table is ever added to. This is proven by comparing each table against its previous Delta
version, row by row and in both directions.

### Five mistakes, and what caught them

Every one of these produced a table that looked fine: full row counts, no duplicates, no errors.

| The mistake | What caught it | The fix |
|---|---|---|
| An estimated car count, 3.9 times too high for Sundbyberg | an outside source | use the measured count from Trafikanalys |
| An approximate median, different on every run | running the pipeline twice | compute it exactly |
| A national average filling a gap for 13 kommuner | a second source | use NOBIL, where nine of them turn out to have no charger |
| A join that never ran, so Kiruna came out milder than Malmö | looking at the map | sort blanks last and check the north |
| A fetch that was read live and never saved | the rerun test | save it like the other sources |

A check that compares the data against itself can pass while being completely wrong. The checks that
caught these compared against an outside source, a previous run or a physical expectation.

### Cost

Azure Cost analysis for 7 September to 6 October 2026:

| Service | SEK |
|---|---|
| Azure Databricks | 518.22 |
| Data Factory | 12.53 |
| Monitor | 5.95 |
| Storage | 4.82 |
| NAT gateway | 0.40 |
| **Total** | **541.93** |

96 percent is Databricks compute. A full pipeline run costs about 1 krona, while an open notebook
costs about 20 kronor an hour whether or not anyone is typing. That is why the pipeline runs as
serverless jobs, once a month.

### Security

- The four API keys live in Key Vault and are read by name.
- The web app reaches the database with its managed identity, so it has no password at all.
- Data Factory can start the jobs and read their status, but cannot edit or delete them.
- Accepted on purpose for a student project: the web page is public, the database firewall allows
  all Azure services, and one password still exists for the Databricks writer account.

### Three things worth taking away

1. Check your numbers against someone else's. Every check on our own data passed, and only an
   outside source found the error.
2. Build a pipeline you can run twice. That is what makes it safe for the system to retry itself.
3. Say which parts are opinions. The weights and the caps are judgements, not measurements.

## Lab 1: ETL with Azure Data Factory

A Data Factory pipeline that copies Swedish municipality and region contact data from Blob Storage
into Azure SQL Database, with reasoning for data types, keys and character encoding. Data Factory
signs in to both ends with its managed identity, so no password or key is stored. The extra task
adds an automatic shutdown of a web app when a budget limit is reached, with alerts by email and
SMS.

## What is not in this repository

Only the documents needed to understand the work are published here. The Databricks notebooks live
in the Databricks workspace. Screenshots, the assignment texts, raw data files and the working
notes that the documents mention, such as `setup_log.md`, are left out.
