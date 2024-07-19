from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.builder.appName('reading_data_from_forecast').master('local[*]').getOrCreate()

# spark.read.format('csv').\
#     option('header',True).\
#     load("C:/Users/kesha/Downloads/world-cup-2022/world-cup-2022/wc_forecasts.csv").\
#     show(vertical=True,truncate=False)

# spark.read.format('csv').\
#     option('header',True).\
#     load("C:/Users/kesha/Downloads/world-cup-2022/world-cup-2022/wc_forecasts.csv").\
#     printSchema()

df=spark.read.format('csv').\
    option('header',True).\
    load("C:/Users/kesha/Downloads/world-cup-2022/world-cup-2022/wc_forecasts.csv")


# df.withColumn('forecast_date',split('forecast_timestamp',' ',)[0])\
#   .withColumn('forecast_time',split('forecast_timestamp',' ')[1])\
#   .withColumn('forecast_zone',split('forecast_timestamp',' ')[2]).show(vertical=True,truncate=False)
#
# df1=df.withColumn('forecast_date',split('forecast_timestamp',' ',)[0])\
#   .withColumn('forecast_time',split('forecast_timestamp',' ')[1])\
#   .withColumn('forecast_zone',split('forecast_timestamp',' ')[2]).\
#   drop('forecast_timestamp').show(2,vertical=True,truncate=False)

df1=df.withColumn('forecast_date',split('forecast_timestamp',' ',)[0])\
  .withColumn('forecast_time',split('forecast_timestamp',' ')[1])\
  .withColumn('forecast_zone',split('forecast_timestamp',' ')[2]).\
  drop('forecast_timestamp')
#
# df1.withColumn('time_stamp_date',split('timestamp',' ')[0])\
#        .withColumn('time_stamp_time',split('timestamp',' ')[1])\
#        .withColumn('time_stamp_zone',split('timestamp',' ')[2]).show(2,vertical=True,truncate=False)


# df1.withColumn('time_stamp_date',split('timestamp',' ')[0])\
#        .withColumn('time_stamp_time',split('timestamp',' ')[1])\
#        .withColumn('time_stamp_zone',split('timestamp',' ')[2]).\
#     drop('timestamp').\
#     show(2,vertical=True,truncate=False)

df2=df1.withColumn('time_stamp_date',split('timestamp',' ')[0])\
       .withColumn('time_stamp_time',split('timestamp',' ')[1])\
       .withColumn('time_stamp_zone',split('timestamp',' ')[2]).\
    drop('timestamp')

# df2.withColumn('forecast_date',trim(col('forecast_date')))\
#     .withColumn('forecast_time',trim(col('forecast_time')))\
#     .withColumn('forecast_zone',trim(col('forecast_zone')))\
#     .withColumn('time_stamp_date',trim(col('time_stamp_date')))\
#     .withColumn('time_stamp_time',trim(col('time_stamp_time')))\
#     .withColumn('time_stamp_zone',trim(col('time_stamp_zone')))\
#     .withColumn('team',trim(col('team')))\
#     .withColumn('group',trim(col('group')))\
#     .withColumn('spi',trim(col('spi')))\
#     .withColumn('global_o',trim(col('global_o')))\
#     .withColumn('global_d',trim(col('global_d')))\
#     .withColumn('sim_wins',trim(col('sim_wins')))\
#     .withColumn('sim_ties',trim(col('sim_ties')))\
#     .withColumn('sim_losses',trim(col('sim_losses')))\
#     .withColumn('sim_goal_diff',trim(col('sim_goal_diff')))\
#     .withColumn('goals_scored',trim(col('goals_scored')))\
#     .withColumn('goals_against',trim(col('goals_against')))\
#     .withColumn('group_1',trim(col('group_1')))\
#     .withColumn('group_2',trim(col('group_2')))\
#     .withColumn('group_3',trim(col('group_3')))\
#     .withColumn('group_4',trim(col('group_4')))\
#     .withColumn('make_round_of_16',trim(col('make_round_of_16')))\
#     .withColumn('make_quarters',trim(col('make_quarters')))\
#     .withColumn('make_semis',trim(col('make_semis')))\
#     .withColumn('make_final',trim(col('make_final')))\
#     .withColumn('win_league',trim(col('win_league')))\
#     .show(3,truncate=False,vertical=True)


df3=df2.withColumn('forecast_date',trim(col('forecast_date')))\
    .withColumn('forecast_time',trim(col('forecast_time')))\
    .withColumn('forecast_zone',trim(col('forecast_zone')))\
    .withColumn('time_stamp_date',trim(col('time_stamp_date')))\
    .withColumn('time_stamp_time',trim(col('time_stamp_time')))\
    .withColumn('time_stamp_zone',trim(col('time_stamp_zone')))\
    .withColumn('team',trim(col('team')))\
    .withColumn('group',trim(col('group')))\
    .withColumn('spi',trim(col('spi')))\
    .withColumn('global_o',trim(col('global_o')))\
    .withColumn('global_d',trim(col('global_d')))\
    .withColumn('sim_wins',trim(col('sim_wins')))\
    .withColumn('sim_ties',trim(col('sim_ties')))\
    .withColumn('sim_losses',trim(col('sim_losses')))\
    .withColumn('sim_goal_diff',trim(col('sim_goal_diff')))\
    .withColumn('goals_scored',trim(col('goals_scored')))\
    .withColumn('goals_against',trim(col('goals_against')))\
    .withColumn('group_1',trim(col('group_1')))\
    .withColumn('group_2',trim(col('group_2')))\
    .withColumn('group_3',trim(col('group_3')))\
    .withColumn('group_4',trim(col('group_4')))\
    .withColumn('make_round_of_16',trim(col('make_round_of_16')))\
    .withColumn('make_quarters',trim(col('make_quarters')))\
    .withColumn('make_semis',trim(col('make_semis')))\
    .withColumn('make_final',trim(col('make_final')))\
    .withColumn('win_league',trim(col('win_league')))\
#
# df3.withColumn('forecast_date',to_date('forecast_date','yyyy-MM-dd'))\
#     .withColumn('forecast_time',to_timestamp('forecast_time','HH:mm:ss'))\
#     .withColumn('time_stamp_date',to_date('time_stamp_date','yyyy-MM-dd'))\
#     .withColumn('time_stamp_time',to_timestamp('time_stamp_time','HH:mm:ss'))\
#     .show(2,vertical=True,truncate=False)



df4=df3.withColumn('forecast_date',to_date('forecast_date','yyyy-MM-dd'))\
    .withColumn('forecast_time',to_timestamp('forecast_time','HH:mm:ss'))\
    .withColumn('time_stamp_date',to_date('time_stamp_date','yyyy-MM-dd'))\
    .withColumn('time_stamp_time',to_timestamp('time_stamp_time','HH:mm:ss'))
#
# df4.withColumn('forecast_time',split(col('forecast_time'),' ')[1])\
#     .withColumn('time_stamp_time',split(col('time_stamp_time'),' ')[1]).\
#     show(2,vertical=True,truncate=False)



df5=df4.withColumn('forecast_time',split(col('forecast_time'),' ')[1])\
    .withColumn('time_stamp_time',split(col('time_stamp_time'),' ')[1])
#
# df5.select(col('forecast_date').cast(DateType()),\
#            col('forecast_time').cast(TimestampType()),\
#            col('forecast_zone').cast(StringType()),\
#            col('team').cast(StringType()),\
#            col('group').cast(StringType()),\
#            col('spi').cast(DoubleType()),\
#            col('global_o').cast(DoubleType()),\
#            col('global_d').cast(DoubleType()),\
#            col('sim_wins').cast(FloatType()),\
#            col('sim_ties').cast(FloatType()),\
#            col('sim_losses').cast(FloatType()),\
#            col('sim_goal_diff').cast(FloatType()),\
#            col('goals_scored').cast(FloatType()),\
#            col('goals_against').cast(FloatType()),\
#            col('group_1').cast(FloatType()),\
#            col('group_2').cast(FloatType()),\
#            col('group_3').cast(FloatType()),\
#            col('group_4').cast(FloatType()),\
#            col('make_round_of_16').cast(FloatType()),\
#            col('make_quarters').cast(FloatType()),\
#            col('make_semis').cast(FloatType()),\
#            col('make_final').cast(FloatType()),\
#            col('win_league').cast(FloatType()),\
#            col('time_stamp_date').cast(DateType()),\
#            col('time_stamp_time').cast(TimestampType()),\
#            col('time_stamp_zone').cast(StringType())).show(2,vertical=True,truncate=False)




df6=df5.select(col('forecast_date').cast(DateType()),\
           col('forecast_time').cast(TimestampType()),\
           col('forecast_zone').cast(StringType()),\
           col('team').cast(StringType()),\
           col('group').cast(StringType()),\
           col('spi').cast(DoubleType()),\
           col('global_o').cast(DoubleType()),\
           col('global_d').cast(DoubleType()),\
           col('sim_wins').cast(FloatType()),\
           col('sim_ties').cast(FloatType()),\
           col('sim_losses').cast(FloatType()),\
           col('sim_goal_diff').cast(FloatType()),\
           col('goals_scored').cast(FloatType()),\
           col('goals_against').cast(FloatType()),\
           col('group_1').cast(FloatType()),\
           col('group_2').cast(FloatType()),\
           col('group_3').cast(FloatType()),\
           col('group_4').cast(FloatType()),\
           col('make_round_of_16').cast(FloatType()),\
           col('make_quarters').cast(FloatType()),\
           col('make_semis').cast(FloatType()),\
           col('make_final').cast(FloatType()),\
           col('win_league').cast(FloatType()),\
           col('time_stamp_date').cast(DateType()),\
           col('time_stamp_time').cast(TimestampType()),\
           col('time_stamp_zone').cast(StringType()))

df6.write.format('bigquery').\
    option('table','dataset.table').\
    option('createDisposition','CREATE_IF_NEEDED').\
    mode('overwrite').\
    save()



