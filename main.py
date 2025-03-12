import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def transform_data(data_source: str, output_uri: str) -> None:
    with SparkSession.builder.appName("Transform Data").getOrCreate() as spark:
        
        # load csv file
        df = spark.read.option("header", "true").csv(data_source)

        # rename columns
        df = df.select(
            col("Name").alias("name"),
            col("Violation Type").alias("violation_type"),
        )

        # create an in-memory table
        df.createOrReplaceTempView("restaurant_violations")

        # construc the query
        GROUP_BY_QUERY = """
            SELECT name, count(*) AS total_red_violations
            FROM restaurant_violations
            WHERE violation_type = 'RED'
            GROUP BY name;
        """
        
        # transform data
        transformed_df = spark.sql(GROUP_BY_QUERY)

        # log in to EMR stdout
        print("Number of rows in sql query: ", transformed_df.count())

        # write our results as parquet file
        transformed_df.write.mode("overwrite").parquet(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_source")
    parser.add_argument("--output_uri")
    args = parser.parse_args()

    transform_data(args.data_source, args.output_uri)

