from pyspark.sql import SparkSession
from datetime import datetime
import sys
from dateutil import parser

date_str = sys.argv[1]

# Creating Spark Session
# Change to yarn for EMR cluster
spark = SparkSession.builder.master("yarn").appName("miterm-project").getOrCreate()

# Read a file from s3
# Change to s3 bucket when running in EMR
sales_df = spark.read.option("header","True").option("delimiter",",").csv(f"s3://midterm-yw/data/de_midterm_raw/sales_{date_str}.csv")
cal_df = spark.read.option("header","True").option("delimiter",",").csv(f"s3://midterm-yw/data/de_midterm_raw/calendar_{date_str}.csv")
inv_df = spark.read.option("header","True").option("delimiter",",").csv(f"s3://midterm-yw/data/de_midterm_raw/inventory_{date_str}.csv")

# By week, store, and product
sales_df.createOrReplaceGlobalTempView("sales")
cal_df.createOrReplaceGlobalTempView("calendar")
inv_df.createOrReplaceGlobalTempView("inventory")

df_sum_sales_quantity = spark.sql("""
    WITH sales_with_inventory AS (
        SELECT
            c.YR_WK_NUM,
            s.store_key,
            s.prod_key,
            TO_DATE(CAST(s.TRANS_DT AS STRING), 'yyyy-MM-dd') AS TRANS_DT,
            s.sales_qty,
            s.SALES_AMT,
            s.SALES_COST,
            i.INVENTORY_ON_HAND_QTY,
            i.INVENTORY_ON_ORDER_QTY,
            i.OUT_OF_STOCK_FLG,
            i.INVENTORY_ON_HAND_QTY * s.SALES_PRICE as stock_on_hand_amt,
            sum(i.OUT_OF_STOCK_FLG) OVER (
                PARTITION BY c.YR_WK_NUM,s.STORE_KEY,s.PROD_KEY
                ORDER BY c.YR_WK_NUM
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as week_out_of_stock,
            last(i.INVENTORY_ON_HAND_QTY) OVER (
                PARTITION BY s.TRANS_DT, s.store_key, s.prod_key
                ORDER BY c.YR_WK_NUM
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as week_end_stock_on_hand,
            last(i.INVENTORY_ON_ORDER_QTY) OVER (
                PARTITION BY c.YR_WK_NUM, s.store_key, s.prod_key
                ORDER BY c.YR_WK_NUM
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as week_end_ordered_stock,
            CASE
                WHEN i.INVENTORY_ON_HAND_QTY < s.SALES_QTY THEN 1
                ELSE 0
            END AS Low_Stock_flg
        FROM 
            global_temp.sales s
        INNER JOIN
            global_temp.calendar c
        ON 
            s.TRANS_DT = c.CAL_DT
        LEFT JOIN
            global_temp.inventory i
        ON 
            i.CAL_DT = s.TRANS_DT AND s.store_key = i.store_key AND s.prod_key = i.prod_key
    ),
    agg_sales_with_inventory AS (
        SELECT
            YR_WK_NUM,
            TRANS_DT,
            store_key,
            prod_key,
            week_end_stock_on_hand,
            week_end_ordered_stock,
            sum(sales_qty) as sum_sales_qty,
            sum(SALES_AMT) as sum_sales_amt,
            round(sum(SALES_AMT) / sum(sales_qty),2) as avg_sales_price,
            sum(SALES_COST) as sum_sales_cost,
            sum(Low_Stock_flg+OUT_OF_STOCK_FLG) as total_Low_Stock_Impact,
            round(sum(case when Low_Stock_flg =1 then (SALES_AMT - stock_on_hand_amt) else 0 end),2) as potential_low_stock_impact,
            sum(CASE WHEN OUT_OF_STOCK_FLG = 1 THEN SALES_AMT ELSE 0 END) as no_stock_impact,
            sum(Low_Stock_flg) as low_stock_instances,
            sum(OUT_OF_STOCK_FLG) as no_stock_instances,
            round(week_end_stock_on_hand/sum(sales_qty),2) as week_supply_stock
        FROM 
            sales_with_inventory
        GROUP BY
            1,2,3,4,5,6
    ),
    agg_sales_with_inventory1 as (
        SELECT
            *,
            lag(sum_sales_amt) OVER (
            PARTITION BY store_key, prod_key
            ORDER BY YR_WK_NUM
            ) as last_week_sales
        From
            agg_sales_with_inventory
    )
    SELECT *,
        round(((sum_sales_amt - last_week_sales) / last_week_sales)*100, 2) as week_over_week_growth,
        CASE WHEN week_end_stock_on_hand = 0 THEN NULL
        ELSE round(sum_sales_qty / week_end_stock_on_hand, 2) END as stock_turnover_rate
    FROM 
        agg_sales_with_inventory1
""")

#df_sum_sales_quantity.show()

# Write the file back to s3
df_sum_sales_quantity.repartition("TRANS_DT").write.mode("overwrite").option("compression","gzip") \
                     .parquet(f"s3://midterm-yw-output/date={date_str}")