# Documentation: Lab 1, ETL with Azure

Collects everything the assignment asks for under "Dokumentera". Screenshots are in `Screenshots/`.

| Resource | Name |
|---|---|
| Subscription | Azure for Students |
| Resource group | `rg-molnkurs-dev-01` |
| Region | Germany West Central |
| Storage account | `stmolnkurst4ngo01` |
| Blob container | `source` |
| SQL server | `sql-molnkurs-t4ngo-01` |
| SQL database | `sqldb-molnkurs-dev-01` |
| Data Factory | `adf-molnkurs-dev-01` |
| Pipeline | `pl_kopiera_kommuner_blob_till_sql` |
| Web app (VG) | `app-molnkurs-t4ngo-01` |
| Logic App (VG) | `logic-molnkurs-dev-01` |

---

## 1. Cost analysis, one month time range, grouped by service

Cost analysis for the subscription *Azure for Students*, time range September 2026, grouped by
**Service name**. No costs had been reported yet for the period.

Screenshot: `labb_1_budget_1.png`

## 2. Budget linked to the account

Budget `lab_1` created at subscription level (*Azure for Students*).

- Amount: 500 SEK, resets monthly
- Alert 1: actual cost reaches 50 % (250 SEK) → email
- Alert 2: forecasted cost reaches 75 % (375 SEK) → email
- Recipient: christofer.lindholm@yh.nackademin.se

500 SEK is roughly half of the student credit ($100 ≈ 952 SEK), so the alerts arrive well before
the credit runs out.

Screenshot: `labb_1_budget_2.png`

## 3. Tag "Moment" on the container

The container `source` in `stmolnkurst4ngo01` has the key `Moment` with the value `Labb1`.
On a blob container the feature is called **Metadata** in the portal (Tags only exist on resources
such as storage accounts and resource groups).

Screenshot: `labb_1_source_3.png`

## 4. File uploaded

`Kontaktuppgifter_kommuner_20250417.csv` (31.18 KiB, block blob, Hot) is uploaded to the container
`source`. The file was uploaded unchanged (encoding ISO-8859-1), because the source area should hold
the data exactly as it was published.

Screenshot: `labb_1_uppload_4.png`

## 5. Logical database server, costs and reasoning behind the choices

Server: `sql-molnkurs-t4ngo-01` in Germany West Central. No screenshot for this step.

**The logical server itself costs nothing.** Costs only arise in the databases created on the
server, so the choices here are mainly about avoiding add-ons that cost money.

| Choice | Reasoning |
|---|---|
| **Microsoft Defender for SQL: off** | Billed per server and month after the trial. Not justified for lab data without sensitive information. |
| **Public endpoint instead of private endpoint** | A private endpoint needs a virtual network and costs extra. A public endpoint is free and enough for a lab environment. |
| **Firewall: only my IP address** | The server is reachable over the internet, but the firewall only lets in my current IP. More IPs can be added later. |
| **"Allow Azure services" on** | Lets Data Factory reach the server later. Downside: it admits all Azure resources, including other customers', but they still need credentials. Acceptable for a lab. |
| **Mixed authentication (SQL + Microsoft Entra ID)** | Costs nothing. Two ways to sign in: SQL login works from any tool, Entra ID signs in with the school account without a password stored in the database. |
| **Region Germany West Central** | The student subscription only allows five regions. This is the closest one with the full range of services. |

## 6. SQL database with tag "Moment"

Database: `sqldb-molnkurs-dev-01` on the server `sql-molnkurs-t4ngo-01`, with the tag `Moment` = `Labb1`.

- **The Azure SQL Database free offer** is used (General Purpose serverless, `GP_S_Gen5_2`). The
  first 32 GB of storage and 100,000 vCore seconds per month are free.
- **When the free quota runs out, the database pauses** until next month instead of being billed
  (Overage billing: Disabled). The portal's cost summary shows 0.00 USD per month.
- **Backups are stored locally redundant**, the cheapest option.
- **Microsoft Defender for SQL is not enabled.**
- **Default collation** (`SQL_Latin1_General_CP1_CI_AS`) was kept. Swedish characters are handled
  with `NVARCHAR` columns (see section 7).

Here "Moment" is a real tag, unlike the container in section 3. A database is an Azure resource,
while a container is data inside a resource and therefore has metadata.

Screenshot: `labb1_sqldb_5.png`

## 7. Data types and reasoning

The table `landing.kommuner` in the schema `landing` receives all ten columns from
`Kontaktuppgifter_kommuner_20250417.csv`. Script: `sql/01_create_landing_kommuner.sql`

```sql
CREATE SCHEMA landing;
GO

CREATE TABLE landing.kommuner (
    kommunkod           CHAR(4)        NOT NULL,
    lanskod             CHAR(2)        NULL,
    organisationsnummer CHAR(11)       NULL,
    namn                NVARCHAR(100)  NULL,
    epost               NVARCHAR(255)  NULL,
    telefon             NVARCHAR(50)   NULL,
    webbadress          NVARCHAR(255)  NULL,
    postadress          NVARCHAR(100)  NULL,
    postnummer          INT            NULL,
    ort                 NVARCHAR(100)  NULL,
    CONSTRAINT PK_kommuner PRIMARY KEY (kommunkod)
);
GO
```

| Column in the file | Column in the table | Data type | Reasoning |
|---|---|---|---|
| Läns-/kommunkod | `kommunkod` | `CHAR(4)` | Always 4 digits. Text, because 90 of 290 codes start with 0 (e.g. 0764), which would disappear in an integer. |
| Länstillhörighet | `lanskod` | `CHAR(2)` | Always 2 digits, can start with 0. |
| Organisationsnummer | `organisationsnummer` | `CHAR(11)` | Fixed format 212000-1439, contains a hyphen. |
| Namn | `namn` | `NVARCHAR(100)` | Contains å, ä, ö. |
| E-post | `epost` | `NVARCHAR(255)` | Free text of varying length. |
| Telefon | `telefon` | `NVARCHAR(50)` | Contains hyphens, spaces and a leading 0, never used in calculations. |
| Webbadress | `webbadress` | `NVARCHAR(255)` | Free text of varying length. |
| Postadress 1 | `postadress` | `NVARCHAR(100)` | Empty for 214 of 290 municipalities. |
| Postnr | `postnummer` | `INT` | Always 5 digits and no Swedish postal code starts with 0 (lowest in the file: 10535). Takes 4 bytes instead of 5. |
| Ort | `ort` | `NVARCHAR(100)` | Contains Å, Ä, Ö. |

**Principles behind the choices:**

- **Codes as text.** Codes are identifiers that nobody calculates with. Integers would remove
  leading zeros. The exception is the postal code, where there is no zero to lose.
- **`CHAR` for fixed length, `NVARCHAR` for the rest.** `NVARCHAR` stores Unicode, so Swedish
  characters are stored correctly regardless of collation.
- **Lengths with margin.** Each length is several times the longest value in the file, so a longer
  name in a new file does not stop the load. For 290 rows the margin costs nothing.
- **Column names without å, ä, ö and spaces.** The names never need brackets and cannot be garbled
  by tools. The link to the file's headers is made in the Data Factory mapping.
- **Only the primary key is NOT NULL.** This is a landing table whose job is to receive the data.
  If a future file has an empty field, the rows should still land, instead of the whole run failing
  with no data to inspect. Quality is checked afterwards with queries, e.g. `WHERE epost IS NULL`.
  The data types still enforce the format.

## 8. Primary key and reasoning

Primary key: `kommunkod` (`CONSTRAINT PK_kommuner PRIMARY KEY (kommunkod)`).

Two columns in the file are unique for all 290 rows: `kommunkod` and `organisationsnummer`.
The choice was `kommunkod`:

- **Official and stable.** The municipality code is the official identifier of a municipality and
  does not change.
- **Contains the county code.** The first two digits always equal the county code (checked for all
  rows). This is needed to add region names on slide 11.
- **Blocks duplicates.** The same municipality cannot be inserted twice, which slide 11 requires.
  A running number column (`IDENTITY`) would not give that protection, since the same municipality
  could then get two different ids.

## 9. Data Factory and access to the source data

Data Factory: `adf-molnkurs-dev-01` (V2) in the resource group `rg-molnkurs-dev-01`, Germany West Central,
with the tag `Moment` = `Labb1`.

- **Without Git.** Changes are saved directly in Data Factory when published with *Publish all*,
  instead of as commits in a repository.
- **System-assigned managed identity is enabled.** The identity has the same name as the resource,
  `adf-molnkurs-dev-01`, and exists as a managed identity in Microsoft Entra ID.
- **Cost:** a Data Factory that runs nothing costs nothing. Billing is per pipeline run and per
  minute of data movement, which for a copy of 290 rows is a fraction of a krona.

**Role assignment**

| Role | Assigned to | Scope |
|---|---|---|
| Storage Blob Data Contributor | `adf-molnkurs-dev-01` (managed identity) | Storage account `stmolnkurst4ngo01` |

- **Managed identity instead of an access key.** Data Factory signs in with its own identity in
  Entra ID. No password or key needs to be stored, so there is nothing that can leak.
- **Why a data role is needed.** Roles such as Owner and Contributor allow managing the resource,
  but not reading the data in it. Storage Blob Data Contributor gives read and write access to the
  blobs.
- **Scope on the storage account, not the resource group.** Data Factory only gets access to the
  account it needs, not to everything added to the resource group later (least privilege).

Screenshot: `labb_1_adf_6.png`

## 10. Database user for Data Factory

For the SQL database there is no role in Access control (IAM) that gives access to the data. The
database controls that itself with its own users and roles. So a user was created in the database
for Data Factory's managed identity. Script: `sql/02_create_adf_user.sql`

```sql
CREATE USER [adf-molnkurs-dev-01] FROM EXTERNAL PROVIDER WITH DEFAULT_SCHEMA = landing;
GO

ALTER ROLE db_owner ADD MEMBER [adf-molnkurs-dev-01];
GO
```

Check in `sqldb-molnkurs-dev-01`, signed in with Microsoft Entra ID:

| anvandare | typ | autentisering | standardschema | roll |
|---|---|---|---|---|
| adf-molnkurs-dev-01 | EXTERNAL_USER | EXTERNAL | landing | db_owner |

- **Same name as Data Factory's managed identity.** `FROM EXTERNAL PROVIDER` looks the name up in
  Microsoft Entra ID, so the user is linked to the identity and no password is created.
- **Default schema `landing`.** Tables given without a schema name are read as
  `landing.<table>`.
- **The `db_owner` role.** Gives all rights in the database, which is simplest in a lab. In
  production `db_datareader` and `db_datawriter` would have been enough, so the identity can read
  and write data but not change or drop tables.
- **Created as the server's Entra admin.** Only a connection through Microsoft Entra ID can create
  users from Entra ID. A first attempt with the SQL login gave error 33159 and succeeded after
  switching to Entra ID.
- **`USE` was not used.** Azure SQL Database cannot switch databases with `USE`, so the query window
  was connected directly to `sqldb-molnkurs-dev-01`.

The screenshot shows the result together with the connection `molnkurs-entra` to
`sql-molnkurs-t4ngo-01.database.windows.net` and the database `sqldb-molnkurs-dev-01`.

Screenshot: `labb_1_users_7.png`

## 11. Linked Services to source and destination

A Linked Service describes where a data store is and how Data Factory signs in. Two are created
and published in `adf-molnkurs-dev-01`:

| Name | Type | Target | Authentication |
|---|---|---|---|
| `ls_blob_source` | Azure Blob Storage | `https://stmolnkurst4ngo01.blob.core.windows.net` (entered manually) | System-assigned managed identity |
| `ls_sql_sink` | Azure SQL Database | `sql-molnkurs-t4ngo-01.database.windows.net`, database `sqldb-molnkurs-dev-01` | System-assigned managed identity |

- **No passwords or keys are stored.** Both sign in with Data Factory's managed identity. Against
  blob storage this works thanks to the Storage Blob Data Contributor role (section 9), against the
  database thanks to the database user (section 10).
- **The firewall rule "Allow Azure services" is needed.** Data Factory's integration runtime
  (AutoResolve) runs on Azure's IP addresses, not mine, and is let in by that rule.
- **Encryption is required** (Encrypt: mandatory) and the server's certificate is verified.
- **Naming:** the prefix `ls_` marks Linked Services, and `source`/`sink` shows which side of the
  copy the service belongs to.
- **Published.** Without Git, changes are only saved with *Publish all*. The button is greyed out in
  the screenshot, which shows nothing unpublished is left.

The screenshot shows both Linked Services in Data Factory `adf-molnkurs-dev-01`, signed in as
christofer.lindholm@yh.nackademin.se (Nackademin AB).

Screenshot: `labb_1_adf_ls_8.png`

## 12. Pipeline run

Pipeline `pl_kopiera_kommuner_blob_till_sql` in the folder `labb1`, with the Copy activity
`cp_kommuner_csv_till_landing`. The source is the dataset `ds_blob_kommuner_csv` and the destination
is `ds_sql_landing_kommuner`.

**Column mapping**

The file's column names differ from the table's, so the mapping is set explicitly:

| Column in the file | Column in the table |
|---|---|
| Läns-/kommunkod | `kommunkod` |
| Länstillhörighet | `lanskod` |
| Organisationsnummer | `organisationsnummer` |
| Namn | `namn` |
| E-post | `epost` |
| Telefon | `telefon` |
| Webbadress | `webbadress` |
| Postadress 1 | `postadress` |
| Postnr | `postnummer` |
| Ort | `ort` |

Everything in a CSV file is text. Type conversion in the Copy activity turns `Postnr` into `int`,
while the municipality code is written unchanged to `char(4)` so leading zeros are kept.

**Run result (debug)**

| Metric | Value |
|---|---|
| Status | Succeeded |
| Files read | 1 (31.93 KB) |
| Rows read | 290 |
| Rows written | 290 |
| Duration | 13 seconds |

The screenshot shows the run in Data Factory `adf-molnkurs-dev-01`, signed in as
christofer.lindholm@yh.nackademin.se (Nackademin AB).

Screenshot: `labb_1_adf_run_9.png`

**Checking the data in the table**

The check queries are in `sql/03_verify_kommuner.sql`. The expected values were counted directly
from the CSV file.

| Check | Expected | Result |
|---|---|---|
| Number of rows | 290 | 290 |
| Rows with a postal address | 76 | 76 |
| Municipality codes starting with 0 | 90 | 90 |
| Broken characters (`�` or `?`) in name and town | 0 | 0 |
| Municipalities whose name starts with Å, Ä or Ö | 23 | 23, e.g. Älmhults kommun, Åmåls kommun, Åre kommun, Örebro kommun |

The broken character check compares in the binary collation `Latin1_General_100_BIN2`. In the
default collation the character `�` has no sort weight and is ignored in comparisons, which makes
an ordinary `LIKE` search match every row. The binary collation compares exact character codes.

Screenshot: `labb_1_tables_10.png`

## 13. Reasoning: Swedish characters in the destination data

Swedish characters can be lost in two places: when the file is **read** and when the data is
**stored**. Both are handled.

**1. Reading: encoding ISO-8859-1 in the source dataset**

The source file is encoded in ISO-8859-1 (checked with `file -i`). In that encoding `ä` is stored
as one byte (`E4`), while UTF-8 uses two bytes (`C3 A4`). Data Factory reads files as UTF-8 by
default. The byte `E4` is then read as an invalid UTF-8 sequence and replaced with the character
`�`, so `Alingsås` becomes `Alings�s`. The damage happens at read time and cannot be repaired in the database.

That is why the encoding in `ds_blob_kommuner_csv` is set to **ISO-8859-1**. That it works is
already visible in the dataset's schema, where the column names from the file's header row were read
correctly as `Läns-/kommunkod` and `Länstillhörighet`. The check query in section 12 also finds no
broken characters in any of the 290 rows.

**2. Storage: NVARCHAR in the table**

The text columns in `landing.kommuner` are `NVARCHAR`, which stores Unicode. Å, ä and ö are
therefore stored correctly regardless of the database collation. With `VARCHAR` it would depend on
the collation's code page whether the characters can be stored, otherwise they are replaced with `?`.

**Why the file was not converted to UTF-8 before upload**

The source area should hold the file exactly as it was published. The encoding is handled in the
pipeline instead and documented in the dataset.

**Observation about sorting**

The database uses the default collation `SQL_Latin1_General_CP1_CI_AS`. The characters are stored
correctly, but sorting does not follow Swedish alphabetical order: in the screenshot, municipalities
starting with Ä and Å are mixed, instead of Å, Ä and Ö coming after Z. So the collation only affects
sort order, not which characters can be stored. Swedish sorting would have required the collation
`Finnish_Swedish_CI_AS`.

## 14. Additional data source: region names for the municipalities

### Source data

**The broken link.** The link in the assignment (`skr.se/skr/tjanster/dataset#esc_entry=55`)
returned HTTP 404: SKR has moved or removed the page. This is how the same data was found again:

1. **The link was interpreted.** The part `esc_entry=55` points to entry number 55 in SKR's data
   catalogue. The catalogue still exists (`catalog.skl.se/store/1/metadata/55`), and entry 55 is
   called "Kontaktuppgifter till regioner".
2. **The dataset was looked up on Sveriges dataportal**, where the municipality file in the lab
   also comes from: https://www.dataportal.se/datasets/653_26843
3. **That it is the same data was checked** in the data portal's metadata. The download links there
   point directly to SKR's catalogue, entry 55, exactly the entry the old link went to. The CSV file
   was downloaded from `https://catalog.skl.se/store/1/resource/169`.
4. **The content was checked** against the assignment: 21 regions with contact details, and all 21
   county codes also appear in the municipality file.

So it is not just similar data, but the same dataset from the same source, published at a
different address.

- **File:** `Kontaktuppgifter-regioner.csv`, 21 rows and 10 columns, semicolon separated,
  encoding ISO-8859-1. Saved unchanged in `raw/` and uploaded to the container `source`.
- **Link to the municipalities:** the column "Läns-/kommunkod" holds a two-digit county code in
  this file. It matches `lanskod` in `landing.kommuner`. Both files contain exactly the same 21
  county codes, so every municipality gets a region.
- **The Region column is used, not Namn.** They only differ for Gotland, where Namn is
  "Region Gotland (region)".
- **Quirks in the source data:** 20 of 21 organisation numbers end with a `?`, one email address
  has a trailing space and the header is spelled "Postaddress 1" with a double d. The data is kept
  as it came in the landing table.

### Extending the database

Script: `sql/04_regioner.sql`

```sql
ALTER TABLE landing.kommuner ADD region NVARCHAR(100) NULL;
GO

CREATE TABLE landing.regioner (
    lanskod             CHAR(2)        NOT NULL,
    namn                NVARCHAR(100)  NULL,
    region              NVARCHAR(100)  NULL,
    organisationsnummer NVARCHAR(20)   NULL,
    epost               NVARCHAR(255)  NULL,
    telefon             NVARCHAR(50)   NULL,
    webbadress          NVARCHAR(255)  NULL,
    postadress          NVARCHAR(100)  NULL,
    postnummer          INT            NULL,
    ort                 NVARCHAR(100)  NULL,
    CONSTRAINT PK_regioner PRIMARY KEY (lanskod)
);
GO
```

- **`region` allows NULL.** The column is added to 290 existing rows that have no value until the
  pipeline has run.
- **`landing.regioner` follows the same principles as `landing.kommuner`:** only the primary key is
  NOT NULL, `NVARCHAR` for Swedish characters and codes as text.
- **`organisationsnummer` is `NVARCHAR(20)`** instead of `CHAR(11)`, because the source's `?` makes
  the value 12 characters long.
- **Primary key `lanskod`:** one row per county, and the same key used for the join.

### Pipeline

The pipeline `pl_kopiera_kommuner_blob_till_sql` has three activities:

| Activity | Type | Source and target | Write behaviour | Depends on |
|---|---|---|---|---|
| `cp_kommuner_csv_till_landing` | Copy data | `ds_blob_kommuner_csv` to `ds_sql_landing_kommuner` | Upsert on `kommunkod` | nothing |
| `cp_regioner_csv_till_landing` | Copy data | `ds_blob_regioner_csv` to `ds_sql_landing_regioner` | Insert, preceded by `TRUNCATE TABLE landing.regioner` | nothing |
| `scr_uppdatera_region` | Script (NonQuery) | `ls_sql_sink` | `UPDATE` as below | both copies, on Succeeded |

The script activity's statement:

```sql
UPDATE k
SET    k.region = r.region
FROM   landing.kommuner k
JOIN   landing.regioner r ON r.lanskod = k.lanskod
WHERE  k.region IS NULL
   OR  k.region <> r.region;
```

- **The copies run in parallel** since they are independent of each other.
- **The update waits for both.** Two dependencies with the condition Succeeded make it run only when
  both copies have succeeded, and never against an empty or half-loaded region table.
- **Checks whether region names are in place.** The `WHERE` condition only updates municipalities
  where the region name is missing or wrong. Other rows are left untouched.
- **SQL in a Script activity instead of a stored procedure.** The logic is a single statement, so a
  separate procedure in the database would have been unnecessary. The whole solution is then visible
  in the pipeline. A procedure was created first but removed for that reason.

### No duplicate municipalities

Duplicates are prevented at three levels:

1. **Upsert on `kommunkod`.** On every run the file's rows are matched against the table on the
   municipality code. Existing municipalities are updated and new ones inserted. A re-run therefore
   creates no duplicates. The column `region` is not part of the mapping and is not overwritten by
   the upsert.
2. **The update never adds rows.** The script activity only runs `UPDATE`.
3. **The primary key `PK_kommuner`.** The database refuses the same municipality code twice. This
   showed in a test run before upsert was introduced, when the copy was stopped with
   "Violation of PRIMARY KEY constraint 'PK_kommuner'" without any row being added.

The region table is instead emptied with `TRUNCATE` before each load. It only holds the file's
content, unlike the municipality table where the column `region` is filled in by the pipeline.

### Runs

Two debug runs of the whole pipeline, both with all three activities Succeeded in
Germany West Central:

| Run | Start | `recordsAffected` in `scr_uppdatera_region` | Meaning |
|---|---|---|---|
| 1 | 2026-09-15 12:08:33 | 290 | every municipality lacked a region and got one |
| 2 | 2026-09-15 12:11:29 | 0 | every region name was already in place, nothing needed changing |

Run 2 shows that the solution can be re-run and only fills in what is missing. The screenshots are
taken from Monitor and show the pipeline's activities, dependencies and run results, signed in as
christofer.lindholm@yh.nackademin.se (Nackademin AB). The message "Pipeline was modified after this
run" is because the pipeline was published after the runs. The configuration is the same.

Screenshot: `labb_1_sollution_1_11.png`

Screenshot: `labb_1_sollution_2_12.png`

### Checking the database

The check queries are in `sql/05_verify_regioner.sql`. The expected values were counted directly
from the two CSV files.

| Check | Expected | Result |
|---|---|---|
| Rows in `landing.kommuner` | 290 | 290 |
| Unique municipality codes | 290 | 290 |
| Municipalities with a region | 290 | 290 |
| Rows in `landing.regioner` | 21 | 21 |
| Regions with municipalities | 21 | 21, e.g. Region Blekinge 5, Region Gävleborg 10, Region Gotland 1 |
| Municipalities in Region Gotland and Region Jämtland Härjedalen | 9 | 9, e.g. Åre kommun, Östersunds kommun, Härjedalens kommun |

- **Gotland has the region name "Region Gotland"** without the suffix "(region)", which shows the
  right column is used.
- **Swedish characters are correct in both columns,** e.g. Region Jämtland Härjedalen and
  Östersunds kommun. The region file is read with ISO-8859-1, just like the municipality file (see
  section 13). An attempt with the encoding ISO-8859-5 (Cyrillic) read the header as
  `Lфns-/kommunkod`, which confirms how decisive the right encoding is.

The screenshot shows the result with the connection `molnkurs-entra` to
`sql-molnkurs-t4ngo-01.database.windows.net`.

Screenshot: `labb_1_verification_13.png`

## 15. VG extra task: automatic shutdown at a budget limit

The goal is for resources to be switched off automatically when the credit is running low, instead
of only sending an email. The chain looks like this:

```
  Budget vg_kreditvakten (75 %)
        │
        ▼
  Action group ag-molnkurs-dev-01  ──▶  email to christofer.lindholm@yh.nackademin.se
        │
        ▼
  Logic App logic-molnkurs-dev-01
        │  HTTP POST .../sites/app-molnkurs-t4ngo-01/stop
        ▼
  The web app app-molnkurs-t4ngo-01 is stopped
```

### Step 1: Budget

| Budget | Amount | Period | Alerts |
|---|---|---|---|
| `vg_kreditvakten` | 950 SEK | annual, 2026-09-01 to 2027-08-31 | Actual cost 50, **75**, 80 and 100 % |
| `vg_test_1sek` | 1 SEK | annual | Actual cost 50, 100 and 300 %, all linked to the action group |

- **The amount equals the credit.** 950 SEK is roughly the 100 USD included in Azure for Students.
- **Annual reset, not monthly.** The credit is a one-off pot valid for twelve months. A monthly
  budget would have reset every month, and the credit could have run out without any alert crossing
  its threshold.
- **Actual cost, not forecasted.** A forecast is a guess. Switching resources off is a drastic
  action that should only happen when the money has really been spent.
- **Only 75 % is linked to the action group.** The assignment's scenario says shut down at 75 %,
  while the step list names alerts at 50, 80 and 100 %. The solution has all four, but
  only 75 % triggers the shutdown. The others send email.

### Step 2: Action group

Action group `ag-molnkurs-dev-01` with the display name `Kreditvakt`:

| Type | Name | Target |
|---|---|---|
| Email | `mail-christofer` | christofer.lindholm@yh.nackademin.se |
| SMS | `mail-christofer` | my mobile number |
| Logic App | `stoppa-kreditvakten` | `logic-molnkurs-dev-01` |

An action group is a reusable list of who should be notified and what should run. The budget only
decides **when**. The action group decides **what happens then**.

The action group fetches the Logic App's callback URL itself when it is picked from the list. The
URL contains a signature and therefore never needs to be copied or stored anywhere else.

### Step 3: Logic App

Logic App `logic-molnkurs-dev-01` (Consumption) in Germany West Central:

| Part | Content |
|---|---|
| Trigger | `When an HTTP request is received` |
| Action | `Stoppa Kreditvakten`, HTTP POST to `https://management.azure.com/.../sites/app-molnkurs-t4ngo-01/stop?api-version=2023-12-01` |
| Authentication | System-assigned managed identity, audience `https://management.azure.com/` |
| Permission | The **Website Contributor** role, on the web app only |

- **The call goes to Azure Resource Manager**, the same API the portal uses when you click Stop on
  a web app.
- **Managed identity instead of a password.** The same principle as Data Factory in section 9.
  Nothing secret is stored in the Logic App.
- **The permission is limited to a single resource.** The Website Contributor role on just
  `app-molnkurs-t4ngo-01` lets the Logic App start and stop the web app, but not touch the database,
  the storage account or Data Factory. The role does not allow changing the App Service plan either,
  so it cannot raise the price tier.
- **The shutdown is a control plane operation.** Stopping a resource is managing the resource,
  unlike Data Factory's data plane role in section 9, which is about the content of the storage.

### The test resource: the Kreditvakten web app

Web app `app-molnkurs-t4ngo-01` on the App Service plan `asp-molnkurs-dev-01`, price tier **Free F1**,
PHP 8.5 on Linux. The page shows two loading bars: how much credit is left and how much of the
student credit's year has passed. It reloads itself every 30 seconds, so a shutdown shows
immediately.

The page was chosen as the test object because it costs 0 kr, can be stopped and starts again with
one click. None of the lab's other resources can be switched off that way: storage accounts and
Data Factory have no stop function, and the serverless database only pauses itself when idle.

**A cost mistake that became a lesson:** the plan was first created on the **B1 Basic** tier, which
costs about 120 SEK a month. It was caught during a check and scaled down to Free F1 the same day.
The hours it ran show up as the 2.31 SEK cost that the test budget reacted to.

### Step 4: Test

**The portal's test feature cannot be used.** The *Test action group* button returns an error, and
the activity log shows the reason:

```
Operation: CreateNotifications
Error code: Conflict
Message:    Free subscription not supported
```

So test notifications are not available on Azure for Students. The limitation is on Azure's side
and not caused by the configuration.

The test was therefore done in two parts:

**Part A: the Logic App was run directly.** The run was started manually in the Azure portal, which
calls the same trigger the action group uses.

| Check | Result |
|---|---|
| Run | Succeeded |
| The action `Stoppa Kreditvakten` | Succeeded, status code 200 from Azure Resource Manager |
| Web app state | Stopped |
| The page in the browser | Azure's page "Error 403 - This web app is stopped" |

This proves the trigger, the managed identity, the role and the shutdown itself.

Screenshot before: `labb_1_vg_alive_14.png`

Screenshot of the run: `labb_1_vg_run_16.png`

Screenshot after: `labb_1_vg_stopped_15.png`

A detail worth noting: the page seemed to stay alive after the shutdown. It is static, so once
loaded it keeps counting in the browser without contact with the server. Only a reload shows that
the app is stopped. That is why the page now reloads itself every 30 seconds.

**Part B: a real budget alert.** The budget `vg_test_1sek` of 1 SEK was created with alerts at 50,
100 and 300 percent, all linked to the action group. Spending was already 2.31 SEK, so the first two
thresholds were passed from the start. The alert would therefore be triggered by Azure itself at the
next evaluation, without any test button. This covers the part that part A does not show: budget to
action group to Logic App.

**Result: the whole chain worked automatically.** In the night to Friday 18 September both alerts
fired, and the web app was switched off without anyone touching it.

| Time (Swedish time) | Event | Evidence |
|---|---|---|
| 16 Sep 09:27 | The budget `vg_test_1sek` is created | |
| 18 Sep 01:56:00 | Azure generates the alert (23:56:00 UTC on the 17th) | email |
| 18 Sep 01:56 | SMS from `Kreditvakt`: spend 2.31 SEK exceeds the threshold 0.5 | SMS |
| 18 Sep 01:56 | Two emails: thresholds 0.50 kr and 1.00 kr exceeded | email |
| 18 Sep 01:56:04 and 01:56:08 | The Logic App runs twice, both Succeeded | run history |
| 18 Sep 01:56:05 and 01:56:08 | *Stop Web App* on `app-molnkurs-t4ngo-01`, caller `logic-molnkurs-dev-01` | activity log |

- **Two runs, one per threshold.** Both the 50 % and the 100 % threshold were passed, so both alerts
  were sent at the same evaluation, four seconds apart. That matches the two emails.
- **The SMS proves the action group was called.** A budget can only send email on its own. SMS can
  only come from an action group, and it is signed with the action group's display name
  `Kreditvakt`.
- **The activity log shows who switched the app off.** The caller is `logic-molnkurs-dev-01`, with
  principal ID `0a385b5d-c59c-4f85-8b57-b4fd7ad7fa16`, i.e. the Logic App's managed identity.
  Neither I nor the portal stopped the app.
- **The runs were not started manually.** The run history shows two runs on the 18th in addition
  to the two manual part A runs on the 16th.

Screenshot SMS: `labb1_kreditvakt_sms_16.jpeg`

Screenshot email: `labb_1_kreditvakt_email_19.png`

Screenshot run history: `labb_1_vg_runs_17.png`

Screenshot activity log: `labb_1_vg_stopped_18.png`

**The delay**

The alert arrived about 40 hours after the budget was created. That matches Microsoft's
documentation: cost data can take up to 72 hours to become available for pay-as-you-go-type
subscriptions, budgets are evaluated every 24 hours, and notifications are sent within an hour of
the evaluation. A budget is therefore not a real-time brake. It reacts after the fact, with a delay
of up to a couple of days.

**An earlier conclusion that turned out to be wrong**

Before the alert arrived, I concluded that it would probably never be sent. The reasons were that
the test button was blocked with "Free subscription not supported", and that Microsoft lists
Azure for Students (`AzureForStudents_2018-01-01`) among offers not supported in Cost Management. In
practice both the budget alert and the action group call worked. The only thing actually missing on
Azure for Students is the test button.

The lesson is that a delay is not the same as something not working. The budget's evaluation cycle
is long, and the right thing was to wait for it before drawing a conclusion.

**Why the action group has no entries in the activity log**

The activity log records management operations, i.e. when someone changes or calls a resource
through Azure Resource Manager. The test button (`CreateNotifications`) is such an operation, which
is why it shows up. A real alert dispatch is not, and therefore does not appear there. The evidence
is instead in the Logic App's run history and the web app's activity log.

**Conclusion**

**The automatic shutdown is verified from start to finish.** When the budget was exceeded, Azure
sent the alert to the action group, which sent email and SMS and called the Logic App. The Logic App
switched off the web app with its managed identity, without any manual intervention.

| Link in the chain | Verified by |
|---|---|
| The budget is evaluated and exceeded | email and SMS with the spend 2.31 SEK |
| The budget calls the action group | SMS signed `Kreditvakt` |
| The action group calls the Logic App | two runs at 01:56:04 and 01:56:08 |
| The Logic App authenticates without a password | the caller is the managed identity |
| The web app is switched off | *Stop Web App* Succeeded, web app Stopped |

The only limitation on Azure for Students is the portal's test button. The test budget
`vg_test_1sek` is removed after the test, since it would otherwise switch the page off at every
evaluation.

**Sources**

- Microsoft Learn: [Tutorial, Create and manage budgets](https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/tutorial-acm-create-budgets), on evaluation frequency and action groups
- Microsoft Learn: [Understand Cost Management data](https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/understand-cost-mgt-data), on data delay and offers
- Microsoft Learn: [Test notification troubleshooting guide](https://learn.microsoft.com/en-us/azure/azure-monitor/alerts/test-action-group-errors)
