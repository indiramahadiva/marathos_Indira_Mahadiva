"""
Bronze layer - raw_country

Ingests the IOC country code reference CSV into bronze.

Data source: LLM-generated IOC country code reference (206 codes), spot-checked
against Wikipedia for major codes (CHI, KOR, GBR, USA, RSA, etc.). Covers all
athlete_country values present in the marathon dataset.
"""

from pyspark import pipelines as dp

BASE_DIR = "/Volumes/marathos/default/raw/country"

schema = (
    spark.read.format("csv")
    .options(header=True, inferSchema=True)
    .load(BASE_DIR)
    .schema
)


@dp.table(
    name="marathos.bronze.raw_country",
    comment="IOC country code -> name + continent reference",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def raw_country():
    return (
        spark.readStream.format("csv")
        .options(header=True, encoding="UTF-8")
        .schema(schema)
        .load(BASE_DIR)
    )