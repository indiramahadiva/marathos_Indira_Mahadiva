"""
Silver layer - marathon_obt

- Split event_distancelength (e.g. '50km') into event_value (50.0) and event_unit ('km').
- Kept only km, mi, and h events.
- Dropped rows where performance format doesn't match event type (DNF, blank, etc).
- Parsed performance into performance_seconds (distance) or performance_km (duration).
- Added event_type ('distance' or 'duration').
- Derived athlete_age and event_start_date.
- Scoped to year_of_event >= 2000.
- Dropped stage race rows (etappe, etape, stage N).
- Recomputed athlete_avg_speed_kmh. Dropped speeds > 30 km/h or = 0, Also drops rows where computed speed is null. 
- Dropped unidentified athletes (XXX country or null birth year).
- Rebuilt athlete_id as xxhash64(source_id, birth_year, gender, country).
- Deduplicated on (athlete_id, event_name, event_dates, performance).
- Used hashing for surrogate IDs — dense_rank not supported on streaming tables.
- Dropped athlete_club, athlete_average_speed, and intermediate columns.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, lit, expr, to_date, split, lower
from utils.utils import rename_columns_to_snake_case


@dp.table(
    name="marathos.silver.marathon_obt",
    comment="Silver layer",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def marathon_obt():
    # Read from bronze + snake_case the columns
    df = spark.readStream.table("marathos.bronze.raw_marathos")
    df = rename_columns_to_snake_case(df)

    # Normalize country codes to uppercase (source has variants like 'swe')
    df = df.withColumn("athlete_country", expr("upper(athlete_country)"))

    # Split "event_distancelength" (e.g. "50km") into value + unit
    df = (df
        .withColumn("event_value", expr("regexp_extract(event_distancelength, '^([0-9.]+)', 1)").cast("double"))
        .withColumn("event_unit", expr("lower(regexp_extract(event_distancelength, '([a-z]+)$', 1))"))
    )

    # Keep only valid rows:
    # km/mi events need a time (hh:mm:ss), h events need a distance (km)
    df = df.filter(
        (col("event_unit").isin("km", "mi") & col("athlete_performance").rlike("^[0-9]+:[0-9]{2}:[0-9]{2}"))
        | ((col("event_unit") == "h") & col("athlete_performance").rlike("^[0-9.]+ km"))
    )

    # Pre-2000 data is sparse and focus >= 2000 (Year)
    df = df.filter(col("year_of_event") >= 2000)

    # Drop multi-day stage races (sub-stages of bigger tour events).
    # guidance: these are partial-race results and add noise to aggregations.
    df = df.filter(~lower(col("event_name")).rlike(r"etappe|etape|étape|\bstage\s+[0-9]"))

    # Parse performance, classify type, add age + ids
    df = (df
        # strip trailing unit text: "2:58:03 h" -> "2:58:03", "245.1 km" -> "245.1"
        .withColumn("perf_clean",
            expr("regexp_extract(athlete_performance, '^([0-9:.]+)', 1)"))
        # distance events: hh:mm:ss -> total seconds
        .withColumn("performance_seconds",
            when(col("event_unit").isin("km", "mi"),
                 expr("split(perf_clean, ':')[0] * 3600 + split(perf_clean, ':')[1] * 60 + split(perf_clean, ':')[2]").cast("bigint")))
        # duration events: numeric km
        .withColumn("performance_km",
            when(col("event_unit") == "h", col("perf_clean").cast("double")))
        .withColumn("event_type",
            when(col("event_unit").isin("km", "mi"), lit("distance")).otherwise(lit("duration")))
        # Recompute athlete avg speed (km/h) from event_value + performance.
        # 'athlete_average_speed' has garbage like 10000 km/h; we ignore it.
        .withColumn("athlete_avg_speed_kmh",
            when(col("event_unit").isin("km", "mi") & (col("performance_seconds") > 0),
                 col("event_value") / (col("performance_seconds") / 3600.0))
            .when((col("event_unit") == "h") & (col("event_value") > 0),
                 col("performance_km") / col("event_value")))
        .withColumn("athlete_age",
            (col("year_of_event") - col("athlete_year_of_birth")).cast("integer"))
        .withColumn("event_start_date",
            to_date(split(col("event_dates"), "-")[0], "dd.MM.yyyy"))
        # Rebuilds the athlete_id column by hashing 4 fields together.
        # Combining: same athlete if source ID + country + gender + birth year all match.
        .withColumn("athlete_id",
            expr("xxhash64("
                 "cast(athlete_id as string), "
                 "coalesce(cast(athlete_year_of_birth as string), ''), "
                 "coalesce(athlete_gender, ''), "
                 "coalesce(athlete_country, ''))"))
        # event_id: stable hash of event name. dense_rank not allowed on streaming.
        .withColumn("event_id",
            expr("CAST(conv(substring(sha2(event_name, 256), 1, 15), 16, 10) AS BIGINT)"))
        .withColumn("date_id",
            expr("CAST(date_format(event_start_date, 'yyyyMMdd') AS INT)"))
        # Result_id: unique per row via xxhash64. monotonically_increasing_id not allowed on streaming.
        .withColumn("result_id",
            expr("xxhash64(event_name, athlete_id, athlete_performance, event_dates)"))
    )

    # Drop rows with implausible speeds: >30 km/h (faster than marathon WR),
    # 0 km/h (DNS/DNF coded as zero), or null.
    df = df.filter(
        (col("athlete_avg_speed_kmh") > 0) & (col("athlete_avg_speed_kmh") <= 30)
    )

    # Drop unidentified athletes - 'XXX' country and null birth year cause different athletes to share the same athlete_id.
    df = df.filter(
        (col("athlete_country") != "XXX")
        & col("athlete_year_of_birth").isNotNull()
    )

    # Deduplicate remaining rows on the full result identity. Catches rare source-data logging duplicates. dropDuplicates is streaming-safe.
    df = df.dropDuplicates(
        ["athlete_id", "event_name", "event_dates", "performance_seconds", "performance_km"]
    )

    # Drop intermediate / unneeded columns
    return df.drop(
        "event_distancelength", "athlete_performance", "perf_clean",
        "athlete_average_speed", "athlete_club"
    )