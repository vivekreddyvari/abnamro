import logging
import os
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import SparkSession
from schemas.schema_enforcement import IngestSchemas
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# logging Information
logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Supported Extensions
SUPPORTED_EXTENSIONS = [
    "csv",
    "json"
]


# Import Necessary Schemas
stocks_schema = IngestSchemas.stocks_schema
clients_schema = IngestSchemas.clients_schema
collateral_schema = IngestSchemas.collaterals_schema

# Paths
stocks_path = "../resources/stocks/stocks.json"
clients_path = "../resources/clients/clients.csv"
collateral_path = "../resources/collateral/collateral.csv"
paths = [stocks_path, clients_path, collateral_path]


class InputFileReader:
    def __init__(self, spark: SparkSession, ingest_path: str, schema: StructType):
        self.spark = spark
        self.ingest_path = ingest_path
        self.schema = schema

    def read_input_file(self) -> DataFrame:
        """ Reads the input data and returns a dataframe"""

        logging.info(f"Reading the file {self.ingest_path}")

        if not self.spark:
            raise ValueError("Spark Session is required ")
        if not self.ingest_path:
            raise ValueError("Ingest path is required ")

        try:
            extension = self.ingest_path.split(".")[-1].lower()

            if extension not in SUPPORTED_EXTENSIONS:
                raise ValueError(f"Unsupported extension: {extension}")

            if extension == "csv":
                input_df = self.spark.read.format("csv").option("header", True).csv(self.ingest_path, schema=self.schema)
                logging.info(f"Successfully read the file {self.ingest_path}")
                return input_df
            elif extension == "json":
                logging.info(f"Reading JSON files")
                input_df = self.spark.read.option("multiline", "true").json(self.ingest_path, schema=self.schema)
                logging.info(f"Successfully read the file {self.ingest_path}")
                return input_df
        except Exception as e:
            logging.error(f"Error in reading the file {e}", exc_info=True)


class ColumnSanitizer:
    @staticmethod
    def sanitize_columns(columns: List[str]) -> List[str]:
        """ Removes spaces with underscore for each column name """
        return [column.replace(" ", "_") for column in columns]


class DataTransformer:
    def __init__(self, input_df: DataFrame):
        self.input_df = input_df

    def transform_data(self) -> DataFrame:
        """ Transform data with clean column names """
        renamed_columns = ColumnSanitizer.sanitize_columns(self.input_df.columns)
        ref_df = self.input_df.toDF(*renamed_columns)
        return ref_df


class OutputFileWriter:
    def __init__(self, ref_df: DataFrame, transformation_path: str):
        self.ref_df = ref_df
        self.transformation_path = transformation_path

    def write_output_file(self) -> None:
        """ Writes the file in parquet format"""
        self.ref_df.write.parquet(os.path.join(self.transformation_path, "collateral_status.parquet"), mode="overwrite")


class CollateralCalculator:
    def __init__(self, clients_df: DataFrame, collateral_df: DataFrame, stocks_df: DataFrame):
        self.clients_df = clients_df
        self.collateral_df = collateral_df
        self.stocks_df = stocks_df

    def calculate_collateral_value(self) -> DataFrame:
        """ Calculates the collateral value for each client """
        joined_df = self.clients_df.join(self.collateral_df, on="client_id", how="left")
        joined_df = joined_df.join(self.stocks_df, on=["symbol", "date"], how="left")
        joined_df = joined_df.withColumn("collateral_value", joined_df.quantity * joined_df.price)
        return joined_df

    def calculate_price_change(self) -> DataFrame:
        """ Calculates the price change for each stock """
        w = Window.orderBy("date").partitionBy("symbol")
        prev_stocks_df = self.stocks_df.withColumn("prev_close", coalesce(lag("low").over(w), lit(0)))
        prev_stocks_df = prev_stocks_df.withColumn("price_change", coalesce((col("low") - col("prev_close")) / col("prev_close"), lit(0)))
        return prev_stocks_df

    def calculate_collateral_fluctuation(self) -> DataFrame:
        """ Calculates the collateral fluctuation for each client """
        collateral_fluctuation_df = self.calculate_collateral_value().join(self.calculate_price_change(), on=["symbol", "date", "high", "low", "open", "price", "volume"], how="left")
        collateral_fluctuation_df = collateral_fluctuation_df.withColumn("collateral_fluctuation", collateral_fluctuation_df.collateral_value - collateral_fluctuation_df.prev_close)
        return collateral_fluctuation_df


class CreditLendingIngest:
    def __init__(self, spark: SparkSession, transformation_path: str):
        self.spark = spark
        self.transformation_path = transformation_path

    def run(self) -> None:
        """ Final Run """
        stocks_reader = InputFileReader(self.spark, stocks_path, stocks_schema)
        stocks_df = stocks_reader.read_input_file()
        logging.info(f" Total stocks {stocks_df.count()}")
        stocks_df.show(5)

        clients_reader = InputFileReader(self.spark, clients_path, clients_schema)
        clients_df = clients_reader.read_input_file()
        logging.info(f" Total clients {clients_df.count()}")
        clients_df.show(5)

        collateral_reader = InputFileReader(self.spark, collateral_path, collateral_schema)
        collateral_df = collateral_reader.read_input_file()
        logging.info(f" Total collaterals {collateral_df.count()}")
        collateral_df.show(5)

        calculator = CollateralCalculator(clients_df, collateral_df, stocks_df)
        collateral_fluctuation_df = calculator.calculate_collateral_fluctuation()
        collateral_fluctuation_df.show(5)

        writer = OutputFileWriter(collateral_fluctuation_df, self.transformation_path)
        writer.write_output_file()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Credit Lending Ingest").getOrCreate()
    output_path = "../output"
    credit_lending_ingest = CreditLendingIngest(spark, transformation_path=output_path)
    credit_lending_ingest.run()