from dataclasses import dataclass
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType


@dataclass
class IngestSchemas:
    """
    This classes consists of several schemas required for the ingestions
    """

    stocks_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("date", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("volume", DoubleType(), True)
    ])

    clients_schema = StructType([
        StructField("client_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True)
    ])

    collaterals_schema = StructType([
        StructField("client_id", IntegerType(), True),
        StructField("collateral_type", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("collateral_price", DoubleType(), True),
        StructField("date", StringType(), True)
    ])










