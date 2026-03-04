0. Assumptions / requirements
Source: Lakeflow Connect target table with SCD_TYPE_2 (so it’s an AUTO CDC / APPLY CHANGES target). 
Table is a streaming table in Unity Catalog.
Reader cluster/warehouse: DBR 15.2+ (needed for CDF over streaming tables, including SCD2 apply_changes targets). 
For SCD2, the logical primary key when interpreting CDF is:
keys + coalesce(__START_AT, __END_AT).  
Let’s call your Connect table:

<catalog>.<schema>.connect_scd2_employees
text

Pattern 1 — DLT / Lakeflow SQL pipeline reading table_changes(...)
Use the Connect table as bronze by surfacing its CDF into a streaming table:

-- Bronze “change” stream from the Lakeflow Connect SCD2 table
CREATE OR REFRESH STREAMING TABLE bronze_employees_changes AS
SELECT *
FROM table_changes('<catalog>.<schema>.connect_scd2_employees', 0);
sql

This reads the change data feed from the SCD2 streaming table starting at version 0 and keeps consuming new changes. 
You then build silver/gold tables off bronze_employees_changes with standard SQL or AUTO CDC INTO (for further SCD1/SCD2 logic as needed).
Pattern 2 — Python Lakeflow / DLT pipeline using readChangeFeed
If your downstream pipeline is Python-based:

from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.temporary_view()
def connect_employees_cdf():
    return (
        spark.readStream
             .format("delta")
             .option("readChangeFeed", "true")
             .option("startingVersion", 0)
             .table("<catalog>.<schema>.connect_scd2_employees")
    )

# Option A: treat the CDF as your bronze streaming table
@dp.table(name="bronze_employees_changes")
def bronze_employees_changes():
    return spark.readStream.table("connect_employees_cdf")
python

Here you’re reading the CDF stream directly from the Connect SCD2 table. 
bronze_employees_changes becomes the head of the medallion; silver/gold can:
Do standard streaming transforms, or
Use dp.create_auto_cdc_flow() to build additional SCD1/SCD2 dimensions from that change stream. 
Pattern 3 — Use CDF → AUTO CDC again for downstream SCD2 dimensions
If you want a domain-specific SCD2 dimension in silver/gold but keep Connect as the raw SCD2 “landing”:

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

@dp.temporary_view()
def employees_cdf():
    return (
        spark.readStream
             .format("delta")
             .option("readChangeFeed", "true")
             .option("startingVersion", 0)
             .table("<catalog>.<schema>.connect_scd2_employees")
    )

dp.create_streaming_table("dim_employees")

dp.create_auto_cdc_flow(
    target="dim_employees",
    source="employees_cdf",
    keys=["employee_id"],
    sequence_by=col("__START_AT"),          # or your business sequence from Connect
    stored_as_scd_type=2                    # SCD Type 2 again
)
python

This gives you:

Connect SCD2 → raw system-of-record history.
dim_employees → curated SCD2 dimension aligned with your model.
Recommended choice for “head of medallion”
For your scenario (“Connect SCD2 as head; downstream DLT consumes changes”), the cleanest pattern is:

Bronze: CDF over the Connect SCD2 table

SQL: CREATE STREAMING TABLE bronze_* AS SELECT * FROM table_changes(...)
or Python: spark.readStream.option("readChangeFeed","true").table(...) in a @dp.table or CREATE STREAMING TABLE.
Silver / Gold: build domain tables and aggregates off that bronze CDF table, optionally using AUTO CDC again where you want SCD1/SCD2 semantics.


Short answer:
__START_AT and __END_AT are system-managed SCD Type 2 validity columns created by AUTO CDC / APPLY CHANGES. They do not cause errors in a downstream DLT/Lakeflow pipeline. Bronze and silver see them as normal columns unless you are creating another SCD2 target with AUTO CDC, in which case they have special meaning for that new target table.

What these columns are
For SCD Type 2 targets:

Pipelines add __START_AT and __END_AT to mark the validity window of each version of a row. 
When you define a target SCD2 streaming table schema manually, you must include these two columns with the same data type as the SEQUENCE BY / sequence_by column. 
For CDF over an SCD2 target, the effective primary key Databricks uses is:

keys + coalesce(__START_AT, __END_AT). 

How they behave in your medallion pipeline
Assume:

Bronze = DLT/Lakeflow table reading from the Lakeflow Connect SCD2 table via CDF (table_changes or readChangeFeed).
Silver = business transforms or additional AUTO CDC dimensions.
Bronze (reading from Connect SCD2 table)
If you do:

CREATE OR REFRESH STREAMING TABLE bronze_changes AS
SELECT * 
FROM table_changes('<catalog>.<schema>.connect_scd2_table', 0);
sql

or in Python:

spark.readStream.format("delta") \
  .option("readChangeFeed", "true") \
  .option("startingVersion", 0) \
  .table("<catalog>.<schema>.connect_scd2_table")
python

then:

__START_AT and __END_AT just come through as ordinary columns in bronze_changes, alongside _change_type, _commit_version, etc. 
DLT does not reinterpret or strip them; it simply persists whatever the CDF emits.
They do not conflict with DLT dataset naming rules and do not cause runtime errors by themselves.
Silver (downstream from bronze)
In silver:

You can select, filter, join, and aggregate on __START_AT/__END_AT like any other columns (for time-travel/point‑in‑time logic, for example).
If silver is just a streaming/mat view, DLT treats these columns as normal; there is no reserved-name error.
If you use AUTO CDC / create_auto_cdc_flow again in silver to build another SCD2 dimension:

Those __START_AT / __END_AT in the source are just data columns.
The target SCD2 table will get its own system-managed __START_AT / __END_AT based on the new sequence_by you specify. 
Common patterns:
Drop or ignore the upstream __START_AT/__END_AT with COLUMNS * EXCEPT (__START_AT, __END_AT) / except_column_list and let the new AUTO CDC manage its own. 
Or reuse upstream __START_AT as sequence_by if that matches your semantics.
Either way, having these columns in the source does not cause errors. Problems only arise if you:

Manually define a target SCD2 schema that omits or mismatches types for __START_AT / __END_AT, or
Try to DML-update a streaming SCD2 target with invalid values in those columns. 
Net‑net for your design
Your Lakeflow Connect SCD2 table can safely be the head of bronze, including its __START_AT / __END_AT.
Downstream DLT/Lakeflow tables (bronze, silver, gold) will not error just because these columns exist.
Treat them as:
System‑managed validity columns on the Connect SCD2 table itself, and
Regular columns when read into downstream tables, unless you explicitly create another SCD2 target with AUTO CDC (in which case they’re inputs you can keep, drop, or reuse, but the new target will manage its own __START_AT / __END_AT).
