from pyspark.sql import SparkSession
from pyspark.sql.functions import *


class CouncilsJob:

    def __init__(self):
        self.spark_session = (SparkSession.builder
                              .master("local[*]")
                              .appName("EnglandCouncilsJob")
                              .getOrCreate())
        self.input_directory = "../assignment_essent/input_data/csv_files/data"

    def extract_councils(self):
        district_df = self.spark_session.read.csv(f"{self.input_directory}/england_councils/district_councils.csv",
                                                  header=True,
                                                  inferSchema=True)
        district_df = district_df.withColumn("council_type", lit("District Council"))

        london_df = self.spark_session.read.csv(f"{self.input_directory}/england_councils/london_boroughs.csv",
                                                header=True,
                                                inferSchema=True)
        london_df = london_df.withColumn("council_type", lit("london borough"))

        metropolitan_df = self.spark_session.read.csv(
            f"{self.input_directory}/england_councils/metropolitan_districts.csv", header=True,
            inferSchema=True)
        metropolitan_df = metropolitan_df.withColumn("council_type", lit("metropolitan district"))

        unitary_df = self.spark_session.read.csv(f"{self.input_directory}/england_councils/unitary_authorities.csv",
                                                 header=True,
                                                 inferSchema=True)
        unitary_df = unitary_df.withColumn("council_type", lit("unitary authority"))

        councils_df = district_df.union(london_df)
        councils_df = councils_df.union(metropolitan_df)
        councils_df = councils_df.union(unitary_df)
        councils_df.show(10)
        print(councils_df.count())
        return councils_df

    def extract_avg_price(self):
        avg_price_df = self.spark_session.read.csv(f"{self.input_directory}/property_avg_price.csv",
                                                   header=True, inferSchema=True)
        avg_price_df = avg_price_df.select(col("local_authority"), col("avg_price_nov_2019"))
        avg_price_df = avg_price_df.withColumnRenamed("local_authority", "council")
        avg_price_df.printSchema()
        return avg_price_df

    def extract_sales_volume(self):
        sales_volume_df = self.spark_session.read.csv(f"{self.input_directory}/property_sales_volume.csv",
                                                      header=True, inferSchema=True)
        sales_volume_df = sales_volume_df.select(col("local_authority"), col("sales_volume_sep_2019"))
        sales_volume_df = sales_volume_df.withColumnRenamed("local_authority", "council")
        sales_volume_df.printSchema()
        return sales_volume_df

    def transform(self, councils_df, avg_price_df, sales_volume_df) -> object:
        """

        :param councils_df: 
        :param avg_price_df: 
        :param sales_volume_df: 
        :return: 
        """
        final_df = councils_df
        final_df = final_df.join(avg_price_df, on='council', how="left")
        final_df = final_df.join(sales_volume_df, on='council', how="left")
        final_df = final_df.select(col("council"), col("county"), col("council_type"), col("avg_price_nov_2019"),
                                   col("sales_volume_sep_2019"))
        final_df.show(316)
        final_df.printSchema()
        print(f" final count {final_df.count()}")
        return final_df

    def run(self):
        return self.transform(self.extract_councils(),
                              self.extract_avg_price(),
                              self.extract_sales_volume())


if __name__ == "__main__":
    cj = CouncilsJob().extract_councils()
    cj_e = CouncilsJob().extract_avg_price()
    cj_es = CouncilsJob().extract_sales_volume()
    cj_t = CouncilsJob().transform(cj, cj_e, cj_es)

    # check the path
    path = "../assignment_essent/input_data/csv_files/data/england_councils"
