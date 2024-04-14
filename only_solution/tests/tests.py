import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.functions import col, lit
from pyspark.sql.window import Window
import os
from data_transformations.ingest import InputFileReader, ColumnSanitizer, DataTransformer, OutputFileWriter, CollateralCalculator, CreditLendingIngest
from schemas import schema_enforcement


class TestInputFileReader(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("TestInputFileReader").getOrCreate()
        self.stocks_path = "../resources/stocks/stocks.json"
        self.clients_path = "../resources/clients/clients.csv"
        self.collateral_path = "../resources/collateral/collateral.csv"
        self.stocks_schema = StructType([
            StructField("symbol", StringType()),
            StructField("date", StringType()),
            StructField("high", DoubleType()),
            StructField("low", DoubleType()),
            StructField("open", DoubleType()),
            StructField("price", DoubleType()),
            StructField("volume", IntegerType())
        ])
        self.clients_schema = StructType([
            StructField("client_id", StringType()),
            StructField("name", StringType()),
            StructField("email", StringType()),
        ])
        self.collateral_schema = StructType([
            StructField("client_id", IntegerType()),
            StructField("collateral_type", StringType()),
            StructField("symbol", StringType()),
            StructField("quantity", IntegerType()),
            StructField("collateral_price", DoubleType()),
            StructField("date", StringType()),
        ])

    def tearDown(self):
        self.spark.stop()

    def test_read_csv(self):
        reader = InputFileReader(self.spark, self.clients_path, self.clients_schema)
        df = reader.read_input_file()
        self.assertEqual(len(df.columns), 3)
        self.assertEqual(df.count(), 390)

    def test_read_json(self):
        reader = InputFileReader(self.spark, self.stocks_path, self.stocks_schema)
        df = reader.read_input_file()
        self.assertEqual(len(df.columns), 7)
        self.assertEqual(df.count(), 42)


class TestColumnSanitizer(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("TestColumnSanitizer").getOrCreate()
        self.columns = ["column 1", "column 2", "column 3"]

    def tearDown(self):
        self.spark.stop()

    def test_sanitize_columns(self):
        sanitized_columns = ColumnSanitizer.sanitize_columns(self.columns)
        self.assertListEqual(sanitized_columns, ["column_1", "column_2", "column_3"])


class TestDataTransformer(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("TestDataTransformer").getOrCreate()
        self.input_df = self.spark.createDataFrame([
            ("A", "B", "C"),
            ("D", "E", "F")
        ], ["column 1", "column 2", "column 3"])
        self.expected_df = self.spark.createDataFrame([
            ("A", "B", "C"),
            ("D", "E", "F")
        ], ["column_1", "column_2", "column_3"])

    def tearDown(self):
        self.spark.stop()

    def test_transform_data(self):
        transformer = DataTransformer(self.input_df)
        transformed_df = transformer.transform_data()
        self.assertEqual(transformed_df.collect(), self.expected_df.collect())


class TestOutputFileWriter(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("TestOutputFileWriter").getOrCreate()
        self.output_path = "../output"
        self.input_df = self.spark.createDataFrame([
            ("A", "B", "C"),
            ("D", "E", "F")
        ], ["column_1", "column_2", "column_3"])

    def tearDown(self):
        self.spark.stop()

    def test_write_output_file(self):
        writer = OutputFileWriter(self.input_df, self.output_path)
        writer.write_output_file()
        self.assertTrue(os.path.exists(os.path.join(self.output_path, "collateral_status.parquet")))


class TestCollateralCalculator(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName("TestCollateralCalculator").getOrCreate()
        self.clients_df = self.spark.createDataFrame([
            ("100010", "Pip RRWTAX", "wrightaoikz@gmail"),
            ("100011", "Electra YGWXUSD", "thomaszbjqtt@yahoo")
        ], ["client_id", "name", "email"])
        self.collateral_df = self.spark.createDataFrame([
            ("100010", "stock", "Adyen", 1000,10000000.31603992,"2024-02-01"),
            ("100010", "stock", "NN", 1000,10000006.936549172,"2024-02-02")
        ], ["client_id", "collateral_type", "symbol", "quantity", "price", "date"])
        self.stocks_df = self.spark.createDataFrame([
            ("NN", "2024-02-01", 100.00, 100.00, 100.00, 100.00, 1000),
            ("AYDEN", "2024-02-02", 100.00, 100.00, 100.00, 100.00, 1000),
            ])

    def tearDown(self):
        self.spark.stop()

    def test_collateral_calculator(self):
        pass
