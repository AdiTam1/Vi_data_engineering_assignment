from pyspark.sql.functions import *
from pyspark.sql.window import Window
from StockAnalysisApp import StockAnalysisApp


if __name__ == '__main__':

    # stock_csv_path parameter:
    stock_csv_path = r"s3a://aws-glue-adit-home-assignment/stock_prices.csv"

    sa_app = StockAnalysisApp(app_name="VI_home_assignment_o1", csv_path=stock_csv_path)
    stock_df = sa_app.create_dataframe()
    if sa_app.check_nulls(df=stock_df, cols="close"):
        stock_df = sa_app.calc_empty_data(df=stock_df, cols="close")

    # get the last close value within the last date for the same ticker
    w_last_date_row_same_ticker = Window.partitionBy("ticker").orderBy("date")
    stock_df = stock_df.withColumn(
        colName="last_close",
        col=lag(
            col="close",
            offset=1
        ).over(w_last_date_row_same_ticker)
    )

    # calculate returns percentage
    stock_df = stock_df.withColumn(
        colName="return",
        col=(col("close")-col("last_close"))/col("last_close")
    )

    # calculate returns percentage avg per day
    returns_avg_per_day_df = (
        stock_df.groupby("date")
        .agg(avg("return").alias("average_return"))
    )

    # print for check stock_df:
    # stock_df.orderBy("date", ascending=True).show()

    # result o1:
    returns_avg_per_day_df.show()

    # create a s3 aws client
    sa_app.s3_create_client()

    # create s3 bucket and folder for current objective

    if not sa_app.s3_is_bucket_exist("aws-glue-adit-home-assignment"):
        sa_app.s3_create_bucket("aws-glue-adit-home-assignment")
    sa_app.s3_create_folder("aws-glue-adit-home-assignment", "o1")

    # upload the result to the s3 bucket and creates a glue table
    sa_app.s3_upload_dataframe(stock_df, "aws-glue-adit-home-assignment", "o1/result")
    sa_app.glue_create_client()
    sa_app.create_glue_table(returns_avg_per_day_df, "o1","s3a://aws-glue-adit-home-assignment/stock_prices.csv")
    sa_app.stop()
