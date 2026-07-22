from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


SOURCE_TABLE = "workspace.instacart_lab.product_master"

PRODUCT_COLUMNS = [
    "product_id",
    "product_name",
    "aisle_id",
    "aisle",
    "department_id",
    "department",
    "is_active",
    "created_at",
    "updated_at",
]


# ------------------------------------------------------------------
# CDF source
# ------------------------------------------------------------------
@dp.view(name="product_cdf_source")
def product_cdf_source():
    """
    Reads the product_master Delta Change Data Feed from version 0.

    Version 0 supplies the initial product snapshot.
    Later versions supply INSERT, UPDATE and DELETE changes.
    """

    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .table(SOURCE_TABLE)

        # AUTO CDC needs the new version of an updated record.
        # The old preimage is not an upsert event.
        .filter(
            col("_change_type").isin(
                "insert",
                "update_postimage",
                "delete",
            )
        )
    )


# ------------------------------------------------------------------
# SCD Type 1 target
# ------------------------------------------------------------------
dp.create_streaming_table(
    name="dim_product_scd1",
    comment=(
        "Current product dimension maintained from product_master "
        "Change Data Feed using AUTO CDC SCD Type 1."
    ),
)


dp.create_auto_cdc_flow(
    name="apply_product_cdf_scd1",

    target="dim_product_scd1",
    source="product_cdf_source",

    keys=["product_id"],

    # Commit timestamp gives SCD2 timestamps that will later support
    # point-in-time fact-to-dimension joins.
    sequence_by=col("_commit_timestamp"),

    apply_as_deletes=expr("_change_type = 'delete'"),

    column_list=PRODUCT_COLUMNS,

    stored_as_scd_type="1",
)


# ------------------------------------------------------------------
# SCD Type 2 target
# ------------------------------------------------------------------
dp.create_streaming_table(
    name="dim_product_scd2",
    comment=(
        "Historical product dimension maintained from product_master "
        "Change Data Feed using AUTO CDC SCD Type 2."
    ),
)


dp.create_auto_cdc_flow(
    name="apply_product_cdf_scd2",

    target="dim_product_scd2",
    source="product_cdf_source",

    keys=["product_id"],

    sequence_by=col("_commit_timestamp"),

    apply_as_deletes=expr("_change_type = 'delete'"),

    column_list=PRODUCT_COLUMNS,

    stored_as_scd_type="2",

    # Only changes to business attributes should create history.
    # Operational audit timestamps are not history-driving attributes.
    track_history_column_list=[
        "product_name",
        "aisle_id",
        "aisle",
        "department_id",
        "department",
        "is_active",
    ],
)