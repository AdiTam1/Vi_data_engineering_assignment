from pyspark.sql.functions import *
from pyspark.sql.window import Window
from StockAnalysisApp import StockAnalysisApp


if __name__ == '__main__':

    # stock_csv_path parameter:
    stock_csv_path = r"s3a://aws-glue-adit-home-assignment/stock_prices.csv"

    sa_app = StockAnalysisApp(app_name="VI_home_assignment_o2", csv_path=stock_csv_path)

    # create df and check for nulls
    stock_df = sa_app.create_dataframe()
    if sa_app.check_nulls(df=stock_df, cols="close"):
        stock_df = sa_app.calc_empty_data(df=stock_df, cols="close")

    # calculate the trade value avg per ticker
    stock_df = stock_df.withColumn("trade_val", col("close") * col("volume"))
    ticker_trade_val_avg = (
        stock_df.groupBy("ticker")
        .agg(avg("trade_val").alias("frequency"))
        .orderBy(desc("frequency"))
    )

    # print for check stock_df:
    # stock_df.orderBy("date", ascending=True).show()

    # result o2:
    ticker_trade_val_avg.show()

    # create a s3 aws client
    sa_app.s3_create_client()

    # create s3 bucket and folder for current objective
    if not sa_app.s3_is_bucket_exist("aws-glue-adit-home-assignment"):
        sa_app.s3_create_bucket("aws-glue-adit-home-assignment")
    sa_app.s3_create_folder("aws-glue-adit-home-assignment", "o2")

    # upload the result to the s3 bucket and creates a glue table
    sa_app.s3_upload_dataframe(stock_df, "aws-glue-adit-home-assignment", "o2/result")
    sa_app.glue_create_client()
    sa_app.create_glue_table(ticker_trade_val_avg, "o2", "s3a://aws-glue-adit-home-assignment/stock_prices.csv")
    sa_app.stop()
