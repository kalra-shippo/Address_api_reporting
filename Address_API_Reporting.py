# Databricks notebook source
# 1. % of Labels purchased passed through address validation
# 2. "incorrect vs correct" percentage of address validation calls

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when, radians, sin, cos, sqrt, asin, col, expr
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

# COMMAND ----------

pip install pandas_ml

# COMMAND ----------

# MAGIC %sql
# MAGIC with txns as
# MAGIC (
# MAGIC SELECT id, object_owner_id, api_rate_id, amount
# MAGIC FROM `prod-catalog`.seg01.api_transaction
# MAGIC where object_created >= date_add(current_date,-110)
# MAGIC and object_created <= date_add(current_date,-10)
# MAGIC and was_test = 'false'
# MAGIC and object_state = 'VALID'
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC rate as
# MAGIC (select r.object_owner_id, r.id as rate_id, r.shipment_id, r.provider_id, r.days, r.servicelevel_id
# MAGIC from `prod-catalog`.seg01.api_rate r
# MAGIC where object_created >= date_add(current_date,-110)
# MAGIC and object_created <= date_add(current_date,-10)
# MAGIC and r.id in (select distinct api_rate_id from txns)
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC rating_surcharge as(
# MAGIC select id, api_rate_id, extra_id, amount from `prod-catalog`.`seg01`.`api_rate_surcharge`
# MAGIC where api_rate_id in (select distinct api_rate_id from txns)
# MAGIC ),
# MAGIC
# MAGIC extra as(
# MAGIC   select ex.id, ex.name from `prod-catalog`.`seg01`.`api_shipment_extra_field` ex
# MAGIC   where ex.id in (select distinct extra_id from rating_surcharge)
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC shipment as(
# MAGIC select id shipment_id, object_owner_id, address_from_id, address_to_id, extra from `prod-catalog`.`seg01`.`api_shipment_202308` 
# MAGIC where object_purpose = 'PURCHASE' and object_created >= date_add(current_date,-110) and object_created <= date_add(current_date,-10)
# MAGIC
# MAGIC union all
# MAGIC select id shipment_id, object_owner_id,address_from_id,  address_to_id, extra from `prod-catalog`.`seg01`.`api_shipment_202309` 
# MAGIC where object_purpose = 'PURCHASE' and object_created >= date_add(current_date,-110) and object_created <= date_add(current_date,-10)
# MAGIC
# MAGIC union all
# MAGIC select id shipment_id, object_owner_id,address_from_id, address_to_id, extra from `prod-catalog`.`seg01`.`api_shipment_202310`
# MAGIC where object_purpose = 'PURCHASE' and object_created >= date_add(current_date,-110) and object_created <= date_add(current_date,-10)
# MAGIC
# MAGIC union all
# MAGIC select id shipment_id, object_owner_id,address_from_id, address_to_id, extra from `prod-catalog`.`seg01`.`api_shipment_202311`
# MAGIC where object_purpose = 'PURCHASE' and object_created >= date_add(current_date,-110) and object_created <= date_add(current_date,-10)
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC address_validator as(
# MAGIC   select id, object_source, validated_address_id from `prod-catalog`.`seg01`.`api_address`
# MAGIC   where object_created >= date_add(current_date,-110)
# MAGIC   and object_created <= date_add(current_date,-10)
# MAGIC   and id IN (SELECT distinct address_to_id FROM shipment)
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC address_from as
# MAGIC (select distinct a.id, a.zip, a.state, a.country_id, z.lat, z.lng 
# MAGIC from `prod-catalog`.seg01.api_address a
# MAGIC left join `intelligence-team`.dev.us_zips z on substr(z.zip,1,5) = substr(a.zip,1,5)
# MAGIC where a.object_created >= date_add(current_date,-110)
# MAGIC and a.id in (select distinct address_from_id from shipment)
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC address_to  as
# MAGIC (SELECT a.id, a.zip, a.state, a.country_id, z.lat, z.lng
# MAGIC FROM `prod-catalog`.seg01.api_address a
# MAGIC left join `intelligence-team`.dev.us_zips z on substr(z.zip,1,5) = substr(a.zip,1,5)
# MAGIC where a.object_created >= date_add(current_date,-110)
# MAGIC and a.id in (select distinct address_to_id from shipment)
# MAGIC ),
# MAGIC
# MAGIC trackables as
# MAGIC (SELECT distinct id, transaction_id, track_status_id
# MAGIC FROM `prod-catalog`.seg01.track_trackable_202311
# MAGIC -- WHERE transaction_id IN (SELECT distinct id FROM txns)
# MAGIC UNION ALL
# MAGIC SELECT distinct id, transaction_id, track_status_id
# MAGIC FROM `prod-catalog`.seg01.track_trackable_202310
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT distinct id, transaction_id, track_status_id
# MAGIC FROM `prod-catalog`.seg01.track_trackable_202309
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT distinct id, transaction_id, track_status_id
# MAGIC FROM `prod-catalog`.seg01.track_trackable_202308
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC track_sub_status as (
# MAGIC   select ts.code error_code, ts.track_status_id from `prod-catalog`.seg01.track_substatus ts
# MAGIC   where track_status_id IN (SELECT distinct track_status_id FROM trackables)
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC track as
# MAGIC (select id,status, status_date, transaction_id, trackable_id
# MAGIC from `prod-catalog`.seg01.api_track_202311
# MAGIC where trackable_id in (select distinct id from trackables)
# MAGIC and is_deleted = 'false'
# MAGIC
# MAGIC UNION ALL
# MAGIC select id,status, status_date, transaction_id, trackable_id
# MAGIC from `prod-catalog`.seg01.api_track_202310
# MAGIC where trackable_id in (select distinct id from trackables)
# MAGIC and is_deleted = 'false'
# MAGIC
# MAGIC UNION ALL
# MAGIC select id,status, status_date, transaction_id, trackable_id
# MAGIC from `prod-catalog`.seg01.api_track_202309
# MAGIC where trackable_id in (select distinct id from trackables)
# MAGIC and is_deleted = 'false'
# MAGIC
# MAGIC UNION ALL
# MAGIC select id,status, status_date, transaction_id, trackable_id
# MAGIC from `prod-catalog`.seg01.api_track_202308
# MAGIC where trackable_id in (select distinct id from trackables)
# MAGIC and is_deleted = 'false'
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC
# MAGIC first_scan as 
# MAGIC (SELECT trackable_id
# MAGIC        ,min(case when status_rank = 1 and status = 'TRANSIT' then status_date else null end) first_scan_date
# MAGIC        ,min(case when status_rank = 1 and status = 'DELIVERED' then status_date else null end) delivered_date
# MAGIC  from
# MAGIC (
# MAGIC   SELECT trackable_id,
# MAGIC         row_number() over (partition by trackable_id, status order by status_date) status_rank,
# MAGIC         status,
# MAGIC         status_date
# MAGIC   from track 
# MAGIC ) Q
# MAGIC group by trackable_id
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC
# MAGIC final as(
# MAGIC select 
# MAGIC tt.transaction_id
# MAGIC , ap.name carrier
# MAGIC , sl.servicelevel_name
# MAGIC , t.amount as txn_amt
# MAGIC , r.rate_id
# MAGIC , r.provider_id
# MAGIC , r.days
# MAGIC , s.shipment_id
# MAGIC , s.object_owner_id
# MAGIC , s.address_to_id
# MAGIC , ts.track_status_id
# MAGIC , av.object_source
# MAGIC , av.validated_address_id
# MAGIC , rs.amount as surcharge_amt
# MAGIC , ex.name as surcharge_name
# MAGIC , da.state destination_state
# MAGIC , sa.country_id source_country
# MAGIC , da.country_id destination_country
# MAGIC , sa.lat as source_lat
# MAGIC , sa.lng as source_lng
# MAGIC , da.lat as destination_lat
# MAGIC , da.lng as destination_lng,
# MAGIC CASE 
# MAGIC   WHEN av.object_source IN ('VALIDATOR') THEN 'was_validated'
# MAGIC   ELSE 'not_validated'
# MAGIC END AS address_validation_flag,
# MAGIC CASE 
# MAGIC   WHEN ts.error_code IN ('address_issue') THEN 'address_issue'
# MAGIC   ELSE 'delivered/all_others_errors'
# MAGIC END AS address_issues_only,
# MAGIC CASE 
# MAGIC   WHEN ts.error_code IN ('address_issue') THEN 'address_issue'
# MAGIC   WHEN ts.error_code IN ('location_inaccessible') THEN 'location_inaccessible'
# MAGIC   WHEN ts.error_code IN ('notice_left') THEN 'notice_left' 
# MAGIC   WHEN ts.error_code IN ('package_lost') THEN 'package_lost'
# MAGIC   WHEN ts.error_code IN ('package_undeliverable') THEN 'package_undeliverable'
# MAGIC   WHEN ts.error_code IN ('return_to_sender') THEN 'return_to_sender'
# MAGIC   ELSE 'delivered/all_others_errors'
# MAGIC END AS delivery_issues
# MAGIC , ROW_NUMBER() OVER (PARTITION BY tt.transaction_id ORDER BY CASE WHEN ts.error_code = 'address_issue' THEN 0 ELSE 1 END, ts.error_code DESC) AS row_num
# MAGIC , datediff(DAY,fs.first_scan_date,fs.delivered_date) days_in_transit
# MAGIC , r.days - days_in_transit as delivery_difference
# MAGIC
# MAGIC from txns t
# MAGIC inner join rate r on r.rate_id = t.api_rate_id
# MAGIC left join rating_surcharge rs on rs.api_rate_id = r.rate_id 
# MAGIC left join extra ex on  ex.id = rs.extra_id
# MAGIC left join trackables tt on tt.transaction_id = t.id
# MAGIC left join track_sub_status ts on ts.track_status_id = tt.track_status_id
# MAGIC left join shipment s on s.shipment_id = r.shipment_id 
# MAGIC left join address_validator av on s.address_to_id = av.id
# MAGIC left join address_from sa on sa.id = s.address_from_id
# MAGIC left join address_to da on da.id = s.address_to_id
# MAGIC left join `prod-catalog`.seg01.adapter_provider ap on ap.id = r.provider_id
# MAGIC left join first_scan fs on fs.trackable_id = tt.id
# MAGIC left join `prod-catalog`.seg01.adapter_servicelevel sl on sl.id = r.servicelevel_id
# MAGIC )
# MAGIC
# MAGIC
# MAGIC select *, 
# MAGIC CASE 
# MAGIC   WHEN delivery_difference < 0 THEN 'Late'
# MAGIC   WHEN delivery_difference == 0 THEN 'On-Time'
# MAGIC   WHEN delivery_difference > 0 THEN 'Early'
# MAGIC   ELSE 'NULL'
# MAGIC END AS transit_metrics
# MAGIC
# MAGIC from final 
# MAGIC WHERE row_num = 1
# MAGIC and source_country = 236
# MAGIC and destination_country = 236

# COMMAND ----------

df1 = _sqldf

# COMMAND ----------

# MAGIC %md
# MAGIC #1. Zone Plot

# COMMAND ----------

# MAGIC # 4. Distance calculation.

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 3958.0  # Earth radius in miles

    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    newlon = lon2 - lon1
    newlat = lat2 - lat1

    haver_formula = sin(newlat/2.0)**2 + cos(lat1) * cos(lat2) * sin(newlon/2.0)**2
    dist = 2 * asin(sqrt(haver_formula))
    mi = R * dist

    return mi

# Apply the UDF to calculate Haversine distance and add a new column
result_df = df1.withColumn("distance_mi", haversine_distance("source_lat", "source_lng", "destination_lat", "destination_lng"))


# COMMAND ----------

# Define conditions and assign values
result_df = result_df.withColumn("zone", 
    when(result_df["distance_mi"] > 1800, "zone_8")
    .when((result_df["distance_mi"] > 1400) & (result_df["distance_mi"] <= 1800), "zone_7")
    .when((result_df["distance_mi"] > 1000) & (result_df["distance_mi"] <= 1400), "zone_6")
    .when((result_df["distance_mi"] > 600) & (result_df["distance_mi"] <= 1000), "zone_5")
    .when((result_df["distance_mi"] > 300) & (result_df["distance_mi"] <= 600), "zone_4")
    .when((result_df["distance_mi"] > 150) & (result_df["distance_mi"] <= 300), "zone_3")
    .when((result_df["distance_mi"] > 50) & (result_df["distance_mi"] <= 150), "zone_2")
    .when(result_df["distance_mi"] < 50, "zone_1")
    .otherwise("zone_NULL")
)

# COMMAND ----------

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.2 State plot

# COMMAND ----------

# address issues by states
country_us = result_df.filter(result_df.destination_country == 236).filter(result_df.delivery_issues != "delivered/all_others_errors")

# COMMAND ----------

display(country_us)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.1. carriers based address issues pivot table

# COMMAND ----------

pivot_df = (result_df.groupBy("carrier", "address_validation_flag")
    .pivot("address_issues_only", values=["address_issue", "delivered/all_others_errors"])
    .count()
    .na.fill(0))

# Calculate total and percentages
total_column = (col("address_issue") + col("delivered/all_others_errors")).alias("total")
percentage_address_issue = ((col("address_issue") / col("total"))*100).alias("percentage_address_issue")
percentage_delivered_errors = ((col("delivered/all_others_errors") / col("total"))*100).alias("percentage_delivered_errors")

# total and percentage columns
result_df_with_totals = pivot_df.withColumn("total", total_column).withColumn("percentage_address_issue", percentage_address_issue).withColumn("percentage_delivered_errors", percentage_delivered_errors)

# COMMAND ----------

display(result_df_with_totals)

# COMMAND ----------

# MAGIC %md
# MAGIC #2.2. carrier based surcharges + percentage of occurances

# COMMAND ----------

from pyspark.sql.functions import sum, count, avg

pivot_surcharge_df = result_df.groupBy("carrier", "address_validation_flag").agg(sum('surcharge_amt').alias("sum_surcharge_amt"), count(when(result_df['surcharge_amt'].isNotNull(), True)).alias('count_amount_not_null'), count(when(result_df['surcharge_amt'].isNull(), True)).alias('count_amount_null'))

# Calculate percentage columns
avg_overall = (col("sum_surcharge_amt") / col("count_amount_not_null")).alias("avg_per_transaction")
total_count = (col("count_amount_not_null") + col("count_amount_null")).alias("total_count")
percentage_overall = (col("count_amount_not_null") / col("total_count") * 100).alias("percentage_overall")
# percentage_specific = (col("transaction_count") / col("transaction_count") * 100).alias("percentage_specific")

# percentage columns to the DataFrame
pivot_surcharge_df_with_percentage = (
    pivot_surcharge_df
    .withColumn("avg_per_transaction", avg_overall)
    .withColumn("total_count", total_count)
    .withColumn("percentage_overall", percentage_overall)
)

# COMMAND ----------

display(pivot_surcharge_df_with_percentage)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Delivery issues confusion matrix

# COMMAND ----------

from sklearn.metrics import jaccard_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import ConfusionMatrixDisplay

import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# import matplotlib.pyplot as plt
confusion_matrix_df = result_df.select("address_validation_flag", "delivery_issues")
confusion_matrix_df = confusion_matrix_df.groupBy("address_validation_flag", "delivery_issues").count()
confusion_matrix_df = confusion_matrix_df.toPandas()
confusion_matrix_df_pivot =  confusion_matrix_df.pivot(index='address_validation_flag', columns='delivery_issues', values='count').fillna(0)

# COMMAND ----------

# Plot the confusion matrix using seaborn
plt.figure(figsize=(8, 6))


confusion_matrix_df_pivot_millions = confusion_matrix_df_pivot / 1e6
for i in range(len(confusion_matrix_df_pivot_millions.index)):
    for j in range(len(confusion_matrix_df_pivot_millions.columns)):
        plt.text(j + 0.5, i + 0.5, f'{confusion_matrix_df_pivot_millions.iloc[i, j]:.2f}M',
                 ha='center', va='center', color='black')

sns.heatmap(confusion_matrix_df_pivot, annot=False, cmap="Blues", cbar=False, linewidths=.4, linecolor = 'k')
plt.xlabel('delivery_issues (Millions)', fontweight='bold', )
plt.ylabel('address_validation_flag (Millions)', fontweight='bold')
plt.title('Confusion Matrix', fontweight='bold')
plt.show()

# COMMAND ----------

from statsmodels.stats.proportion import proportions_chisquare
import numpy as np
success_cnts = np.array([970000, 180000])
total_cnts = np.array([47200000, 13560000])
chi2, p_val, cont_table = proportions_chisquare(count=success_cnts, nobs=total_cnts)

print('Chi-square statistic = {:.2f}'.format(chi2))
print('p-value = {:.4f}'.format(p_val))

# COMMAND ----------

# MAGIC %md
# MAGIC #4. Identify null values at each service level

# COMMAND ----------

from pyspark.sql import functions as F

column_to_count = "days"
df1 = df1.na.drop(subset=["servicelevel_name"])

# Count the number of null values for each category
count_summary = df1.groupBy("carrier", "servicelevel_name").agg(
    F.count(F.when(F.col(column_to_count).isNull(), True)).alias(f"null_count_{column_to_count}"),
    F.count(F.when(F.col(column_to_count).isNotNull(), True)).alias(f"non_null_count_{column_to_count}")
)

# COMMAND ----------

display(count_summary)

# COMMAND ----------

# find the median value to replace null values

# COMMAND ----------


# Remove rows with null values in the 'days' column
df_dropped = df1.na.drop(subset=["days"])

# Perform aggregation
result = df_dropped.groupBy("carrier","servicelevel_name").agg(F.mean("days").alias("mean_value"),
    F.expr("percentile_approx(days, 0.5)").alias("median_value"),
    F.expr("percentile_approx(days, 0.9)").alias("90_median_value")
)

# COMMAND ----------

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Replacing NULL with 90 Percentile values for same carriers.

# COMMAND ----------

def fill_null_with_median(df, column_name):
    window_spec = Window().partitionBy("carrier", "servicelevel_name")
    
    # Calculate the median value for each group
    median_values = df.groupBy("carrier", "servicelevel_name").agg(
        F.expr(f"percentile_approx({column_name}, 0.9)").alias("median_c")
    )

    # Join the original DataFrame with the calculated median values
    df_with_median = df.join(median_values, on=["carrier", "servicelevel_name"], how="left")

    # Replace null values in the specified column with the corresponding median value
    df_with_median = df_with_median.withColumn(
        column_name, F.when(df_with_median[column_name].isNull(), df_with_median["median_c"]).otherwise(df_with_median[column_name])
    )

    df_with_median = df_with_median.drop("median_c")
    return df_with_median

# Apply the function to "days_in_transit"
result_df = fill_null_with_median(result_df, "days_in_transit")

# Apply the function to "days"
result_df = fill_null_with_median(result_df, "days")

# COMMAND ----------

column1 = "days"
column2 = "days_in_transit"

result_df = result_df.withColumn("delivery_difference", expr(f"{column1} - {column2}"))

# COMMAND ----------

result_df = result_df.withColumn("transit_metrics", 
                     when(result_df["delivery_difference"] < 0, "Late")
                    .when(result_df["delivery_difference"] == 0, "On-Time")
                    .when(result_df["delivery_difference"] > 0, "Early")
                    .otherwise("null"))

# COMMAND ----------

display(result_df)

# COMMAND ----------


