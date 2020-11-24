#install.packages(c('dbplot','mlflow'))
#library(mlflow)

#Sys.setenv(MLFLOW_PYTHON_BIN="databricks/conda/envs/databricks-ml/bin/python")
#install_mlflow()
#help(install_mlflow)

library(sparklyr)
library(dplyr)

options(repr.plot.width=900, repr.plot.height=400)

SparkR::sparkR.session()
sc <- spark_connect(method="databricks")
#options(repr.plot.width=900, repr.plot.height=400)

taxi <- tbl(sc, 'dave_carlson_databricks_com_db.taxi_trips')

taxi_rides_by_month <- taxi %>%
  mutate(trip_month = date_format(trip_start_timestamp, 'yyyy-MM')) %>%
  group_by(trip_month) %>%
  count() %>%
  arrange(trip_month)

barplot(n ~ trip_month, data=taxi_rides_by_month, col='blue')

#library(ggplot2)
library(dbplot)

taxi %>% dbplot:::dbplot_boxplot(payment_type, tips)
taxi %>% dbplot_bar(payment_type)

taxi_tips <- taxi %>%
  filter(payment_type == 'Credit Card') %>%
  mutate(total = trip_total - tips, tip_pct = tips / total) %>%
  filter(total > 0)

taxi_tips %>% sdf_quantile('tip_pct', probabilities=c(0.98, 0.99, 0.995, 0.999))

tip_threshold <- taxi_tips %>% sdf_quantile('tip_pct', probabilities=0.995) %>% as.numeric

taxi_cleaned <- taxi %>%
  filter(payment_type == 'Credit Card') %>%
  mutate(total = trip_total - tips, tip_pct = tips / total) %>%
  filter(total > 0) %>%
  mutate(
    trip_year = year(trip_start_timestamp),
    trip_month = month(trip_start_timestamp),
    trip_hour = hour(trip_start_timestamp),
    trip_dayofweek = dayofweek(trip_start_timestamp),
    tip_pct = if_else(tip_pct > tip_threshold, tip_threshold, tip_pct)) %>%
  select(trip_year, tip_pct, trip_seconds, trip_miles, total, trip_month, trip_hour, trip_dayofweek, 
         pickup_centroid_latitude, pickup_centroid_longitude, pickup_centroid_location, 
         dropoff_centroid_latitude, dropoff_centroid_longitude)

train <- taxi_cleaned %>% filter(trip_year %in% c(2017, 2018))
test  <- taxi_cleaned %>% filter(trip_year == 2019)

help('spark_write_delta')
sparklyr::spark_write_delta(train, path='dbfs:/home/dave.carlson@databricks.com/datasets/delta/chicago/taxi-train', mode='overwrite')
