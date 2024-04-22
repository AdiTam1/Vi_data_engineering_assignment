from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import boto3
import os


class StockAnalysisApp:

    def __init__(self, app_name: str, csv_path: str) -> None:
        """
           Constructor method for initializing the StockAnalysisApp.

           Parameters:
               :param app_name: Name of the Spark application.
               :param csv_path: Path to the CSV file containing stock data.
        """

        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        self.__path = csv_path
        self.s3_client = None
        self.glue_client = None

        self.spark.sparkContext.setLogLevel(logLevel="WARN")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "")

    def create_dataframe(self) -> DataFrame:
        """ creates a DataFrame from the input csv """

        # define stocks schema:
        cols_names = ["date", "open", "high", "low", "close", "volume", "ticker"]
        cols_types = [DateType(), DoubleType(), DoubleType(), DoubleType(), DoubleType(), LongType(), StringType()]
        schema_cols = [StructField(name, data_type, True) for name, data_type in zip(cols_names, cols_types)]
        schema = StructType(schema_cols)

        # create & return stocks dataframe
        return self.spark.read.csv(path=self.__path, header=True, schema=schema, dateFormat="M/d/yyyy")

    @staticmethod
    def check_nulls(df: DataFrame, cols: str) -> bool:

        # check for null values in specified columns of the input df

        if type(cols) == str:
            cols = [cols]
        for c in cols:
            null_count = df.filter(col(c).isNull()).count()
            if null_count > 0:
                return True
        return False

    @staticmethod
    def calc_empty_data(df: DataFrame, cols: str) -> DataFrame:

        # fill empty values in the input columns with the previous non-null value

        if type(cols) == str:
            cols = [cols]
        w_last_date_row_same_ticker = Window.partitionBy("ticker").orderBy("date")
        for c in cols:
            df = df.withColumn(
                colName=c,
                col=lag(col=col(cols), offset=1).over(w_last_date_row_same_ticker)
            )
        return df

    def stop(self) -> None:

        # stops the spark session and spark context

        self.spark.stop()
        self.spark.sparkContext.stop()

    def s3_create_client(self) -> None:

        # creates a s3 aws client

        access_key = ""
        secret_access_key = ""
        client = boto3.client('s3', aws_secret_access_key=secret_access_key, aws_access_key_id=access_key)
        self.s3_client = client

    def s3_create_bucket(self, bucket_name: str) -> None:

        # create s3 bucket

        self.s3_client.create_bucket(Bucket=bucket_name)

    def s3_is_bucket_exist(self, bucket_name: str) -> bool:

        # check if the input s3 bucket exists

        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            return True
        except Exception as e:
            print(e)
            return False

    def s3_upload_file(self, bucket_name: str, file_path: str) -> None:

        # upload a file to the input s3 bucket

        file_name = os.path.basename(file_path)
        if os.path.isfile(file_path):
            try:
                with open(file_path, 'rb') as f:
                    self.s3_client.upload_fileobj(f, bucket_name, file_name)
            except Exception as e:
                print(e)
        else:
            print("Error: file not found")

    def s3_create_folder(self, bucket_name: str, file_path: str) -> None:

        # create a folder in the inpt s3 bucket

        try:
            self.s3_client.put_object(Bucket=bucket_name, Key=file_path + "/", Body=b'')
        except Exception as e:
            print(e)

    def s3_upload_dataframe(self, df: DataFrame, bucket_name: str, file_path: str) -> None:

        # upload a dataframe to the input s3 bucket

        df.write.parquet(f"s3a://{bucket_name}/{file_path}")

    @staticmethod
    def save_df(df, result_path: str) -> None:

        # saves a dataframe to the input path

        df.write.format("csv").option("header", True).save(result_path)

    def glue_create_client(self):

        # creates a glue client

        access_key = ""
        secret_access_key = ""
        client = boto3.client('glue', aws_secret_access_key=secret_access_key, aws_access_key_id=access_key,
                              region_name="us-east-1")
        self.glue_client = client

    def create_glue_table(self, df, table_name, location):

        # creates a glue table from the input DataFrame

        columns = []
        for field in df.schema.fields:
            column_name = field.name
            column_type = field.dataType.simpleString()
            columns.append({"Name": column_name, "Type": column_type})

        # create the table definition
        table = {
            "Name": table_name,
            "DatabaseName": "adi_home_assignment",
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {
                "Location": location,
                "InputFormat": "org.apache.spark.sql.parquet.ParquetInputFormat",
                "OutputFormat": "org.apache.spark.sql.parquet.ParquetOutputFormat",
                "SerdeInfo": {
                    "TypeName": "struct<" + ", ".join([f"{c['Name']}:{c['Type']}" for c in columns]) + ">"
                }
            },
            "Columns": columns
        }
        self.glue_client.create_table(DatabaseName="aws-glue-adit-home-assignment", TableInput=table)
