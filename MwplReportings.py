# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.arulrajstorageaccount.blob.core.windows.net",
    "NxpKy/qQajn513DiRMBMP1JHAG+N7FKHSZ4fxsJQmLtk1I5mA6N5UmKdbKKxu+uSzwiDnkv7Dkpf+AStUFFGlg=="
)

# COMMAND ----------

from pyspark.sql.functions import to_date, col

# COMMAND ----------

read_df = spark.read\
        .format("csv")\
        .option("Header",True)\
        .load("wasbs://private@arulrajstorageaccount.blob.core.windows.net/capital_market/openinterest/Mar2023.csv")

# COMMAND ----------

col_mapping = {
    "Date" : "date",
    "ISIN":"isin",
    "`Scrip Name`":"scrip_name",
    "`NSE Symbol`":"nse_symbol",
    "MWPL":"mwpl",
    "`Open Interest`":"open_interest",
    "`Limit for Next Day`":"limit_for_next_day"
}

schema = {
    "mwpl":"INT",
    "open_interest":"INT",
    "limit_for_next_day":"INT"
}

# COMMAND ----------

def col_renaming(df, col_mapping):
    lst = []
    for i,j in col_mapping.items():
        lst.append(f"{i} as {j}")
    
    renamed_df = df.selectExpr(*lst)

    return renamed_df

def data_type_conversion(df, schema):
    for col in schema:
        df = df.withColumn(col, df[col].cast(schema[col]))
    return df
    

def cleansing_df(df, schema, col_mapping):
    return data_type_conversion(col_renaming(df, col_mapping), schema)

# COMMAND ----------

cleaned_df = cleansing_df(read_df, schema, col_mapping)
final_df = cleaned_df.withColumn("date", to_date(col("date"), "dd-MMM-yy"))

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore;
# MAGIC CREATE DATABASE IF NOT EXISTS capital_market;
# MAGIC USE capital_market;
# MAGIC
# MAGIC DROP TABLE IF EXISTS hive_metastore.capital_market.open_interest;
# MAGIC
# MAGIC CREATE TABLE hive_metastore.capital_market.open_interest
# MAGIC (
# MAGIC date DATE,
# MAGIC isin STRING,
# MAGIC scrip_name STRING,
# MAGIC nse_symbol STRING,
# MAGIC mwpl INT,
# MAGIC open_interest INT,
# MAGIC limit_for_next_day INT
# MAGIC )
# MAGIC USING delta
# MAGIC

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("hive_metastore.capital_market.open_interest")

# COMMAND ----------

# MAGIC %sql
# MAGIC with temp as (
# MAGIC   select 
# MAGIC A.Date as Date1,
# MAGIC min(B.Date) as Date2,
# MAGIC A.nse_symbol as nse_symbol
# MAGIC from hive_metastore.capital_market.open_interest A, hive_metastore.capital_market.open_interest B
# MAGIC where A.Date < B.date
# MAGIC and A.nse_symbol = B.nse_symbol
# MAGIC group by A.Date , A.nse_symbol
# MAGIC )
# MAGIC
# MAGIC
# MAGIC select
# MAGIC C.Date1, 
# MAGIC C.Date2, 
# MAGIC C.nse_symbol, 
# MAGIC cast((D.limit_for_next_day/D.mwpl*100) - (E.limit_for_next_day/E.mwpl*100) as dec(5,2)) as buying_pressure
# MAGIC from temp as C 
# MAGIC inner join hive_metastore.capital_market.open_interest D
# MAGIC on C.Date1 = D.Date
# MAGIC inner join hive_metastore.capital_market.open_interest E
# MAGIC on C.Date2 = E.Date
# MAGIC where C.nse_symbol = D.nse_symbol
# MAGIC and C.nse_symbol = E.nse_symbol
# MAGIC and (D.limit_for_next_day/D.mwpl*100)-(E.limit_for_next_day/E.mwpl*100) > 5
# MAGIC order by C.Date1, C.Date2 desc
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


