from pyspark.sql.functions import *
from pyspark.sql.window import Window
from StockAnalysisApp import StockAnalysisApp

if __name__ == '__main__':

    # stock_csv_path parameter:
    stock_csv_path = r"s3a://aws-glue-adit-home-assignment/stock_prices.csv"

    # create StockAnalysisApp that's open spark session
    sa_app = StockAnalysisApp(app_name="VI_home_assignment_o4", csv_path=stock_csv_path)

    # create df and check for nulls
    stock_df = sa_app.create_dataframe()
    if sa_app.check_nulls(df=stock_df, cols="close"):
        stock_df = sa_app.calc_empty_data(df=stock_df, cols="close")
    stock_df = stock_df.withColumn("date_30_days_ago", date_sub(col("date"), 30))

    # join the df with itself for the past 30 days
    joined_df = (stock_df.alias("df1").join(
        other=stock_df.alias("df2"),
        on=(
                   col("df1.date_30_days_ago") == col("df2.date")) &
           (col("df1.ticker") == col("df2.ticker")
            ),
        how="left"
    ).select(
        col("df1.date"),
        col("df1.close"),
        col("df1.ticker"),
        col("df1.date_30_days_ago"),
        col("df2.close").alias("prev_close")
    )

        # calc the 30 days retur

    )
    joined_df = joined_df.withColumn(
        colName="30_days_return",
        col=(col("df1.close") - col("prev_close")) / col("prev_close")
    )
    # calc the rank of returns within each ticker group
    w_30_days_return_part_ticker_date = Window.partitionBy("ticker").orderBy(desc("30_days_return"))
    joined_df = joined_df.withColumn(
        colName="return_rank",
        col=row_number().over(w_30_days_return_part_ticker_date)
    )
    joined_df = joined_df.where("return_rank <= 3")
    joined_df.show()

    # create a s3 aws client
    sa_app.s3_create_client()
    # create s3 bucket and folder for current objective

    if not sa_app.s3_is_bucket_exist("aws-glue-adit-home-assignment"):
        sa_app.s3_create_bucket("aws-glue-adit-home-assignment")
    sa_app.s3_create_folder("aws-glue-adit-home-assignment", "o4")

    # upload the result to the s3 bucket and creates a glue table
    sa_app.s3_upload_dataframe(stock_df, "aws-glue-adit-home-assignment", "o4/result")
    sa_app.glue_create_client()
    sa_app.create_glue_table(joined_df, "o4", "s3a://aws-glue-adit-home-assignment/stock_prices.csv")
    sa_app.stop()
