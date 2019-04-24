from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
import time


##########################################################################################################

def gen_gps_dataframe(df, datetime_format, selection=None, offset=-6):
    """
        GENERATE GPS DATAFRAME
        
        Input parameters:
                - df:                      the Spark dataframe object containing the GPS raw data
                - datetime_format:         the datetime format of raw data
                - selection:               the list of features to be excluded 
                - offset:                  the offset between UTC and LOCAL time
        
    """
    
    # Convert column names to lowercase and remove spaces
    gps_data = df.toDF(*[c.lower().strip().replace(' ', '_') for c in df.columns])
    gps_data.cache()
    
    # Drop duplicates
    gps_data = gps_data.drop_duplicates()
    
    header = gps_data.columns 
    
    # Rename third and fourth columns, which define date and time, respectively
    # Remove third and fourth columns
    gps_data = gps_data.withColumn("date", F.col(header[2]))
    gps_data = gps_data.drop(header[2])
    gps_data = gps_data.withColumn("time", F.col(header[3]))
    gps_data = gps_data.drop(header[3])
    
    # Define latitude and longitude with sign and remove duplicated columns
    gps_data = gps_data.withColumn('lat', 
                                   F.when(
                                       gps_data['n/s']=='S', F.col('latitude')*(-1)
                                   ).otherwise(F.col('latitude'))
                                  ).drop('latitude').drop('n/s')
    
    gps_data = gps_data.withColumn('lon', 
                                   F.when(
                                       gps_data['e/w']=='W', F.col('longitude')*(-1)
                                   ).otherwise(F.col('longitude'))
                                  ).drop('longitude').drop('e/w')
        
    # Define timestamps
    gps_data = gps_data.withColumn('timestamp', 
                                   F.to_timestamp(
                                       F.concat(
                                           F.col('date'),
                                           F.lit(' '),
                                           F.col('time')
                                       ), datetime_format
                                   )
                                  )
    
    # Apply time offset
    gps_data = gps_data.withColumn('timestamp', 
                                   gps_data.timestamp + F.expr('INTERVAL {} HOURS'.format(str(offset)))
                                  )
    
    # Calculate day of the week
    gps_data = gps_data.withColumn('dow',
                                   F.date_format('timestamp','u')
                                   )
    
    drop_list = ['date', 'time']
    drop_list.extend(selection)

    gps_data = gps_data.select(
        [column for column in gps_data.columns if column not in drop_list]
    ).orderBy('timestamp')
    
    return gps_data

##########################################################################################################

def queryrows(df, string):
    """
        Select the rows which match a given string 
    """
    filter_value = df.schema.names[0] + " like '%" + string + "%'"
    
    return df.filter(filter_value).collect()[0][0]

##########################################################################################################

def gen_acc_dataframe(df, datetime_format):
    """
        GENERATE ACCELEROMETER DATAFRAME
        
        Input parameters:
                - df:                      Spark dataframe object containing the acc. raw data
                - datetime_format:         datetime format of raw data
        
    """
    
    # Extract metadata from RDD object
    start_date = queryrows(df, 'Start Date').split()[2]
    start_time = queryrows(df, 'Start Time').split()[2]
    interval = queryrows(df, 'Epoch Period').split()[3]
    end_time = queryrows(df, 'Download Time').split()[2]
    end_date = queryrows(df, 'Download Date').split()[2]
    
    start_timestamp = datetime.strptime(start_date + " " + start_time, datetime_format)
    end_timestamp = datetime.strptime(end_date + " " + end_time, datetime_format)
    x = time.strptime(interval, '%H:%M:%S')
    interval_sec = timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec)
    
    #start_timestamp = start_timestamp + interval_sec # measurements start at the first epoch #<<<<<<<<<<<<<<
    
    # Extract accelerometer data from RDD object
    acc_data = df.filter("value like '%,%'")  # TODO: change 'value' as 'df.schema.names[0]'
    acc_data.cache()
    acc_data = acc_data.selectExpr('value as acc_data') # change column name to 'acc_data'
    acc_data = acc_data.withColumn('id', F.monotonically_increasing_id())
    
    #tot_intervals = acc_data.count()         #<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    #timestamps = [start_timestamp + k*interval_sec for k in range(tot_intervals)]
    
    ## implementation with Pandas dataframe -- slow, not use it
    #acc_data = acc_data.toPandas()
    #acc_data['timestamp'] = timestamps
    #acc_data = spark.createDataFrame(acc_data).select(['timestamp', 'acc_data'])
    
    ## the following lines do not work in a multicore setting because only data loaded on one 
    ## core will bbe retrieved 
    #time_col = Row("timestamp")
    #time_rdd = spark.sparkContext.parallelize(timestamps)
    #time_df = time_rdd.map(time_col).toDF()
    #time_df = time_df.withColumn('id', F.monotonically_increasing_id())
    
    ## the same problem as above withthe following solution
    #time_df = spark.createDataFrame(timestamps, TimestampType()).selectExpr('value as timestamp')
    #time_df = time_df.withColumn('id', F.monotonically_increasing_id())
    #acc_data = acc_data.join(time_df, 'id').drop('id').select(['timestamp','acc_data'])
    
    app_fun = F.udf(lambda k: start_timestamp + k*interval_sec, TimestampType())
    acc_data = acc_data.withColumn('timestamp', app_fun(acc_data['id'])
                                  ).select(['timestamp','acc_data'])
        
    return interval_sec.seconds, acc_data

##########################################################################################################

def split_acc_data(df, col_list):
    """
        Reads string of data from accelerometer dataframe
        
        Input:
              - df:                 Spark dataframe object containing accelerometer data
              - col_list            list of features names matching accelerometer data
    """
    
    df.cache()
    cols = F.split(df['acc_data'], r',')
   
    
    for k, item in enumerate(col_list):
        df = df.withColumn(item, cols.getItem(k).cast(dataType=IntegerType()))
    df = df.drop('acc_data')

    return df

##########################################################################################################

def activity_count(df, data_col, datetime_col, interval, NWTIME=90, TOL=2):
    
    """
        Return activity count calculated from accelerometer data
        
        Input:
              - df:                Spark dataframe object containing accelerometer data
              - data_col:          name of column with list of accelerometer data
              - datetime_col:      name of column with timestamps
              - interval:          duration of a single epoch (in seconds)
              - NOWTIME:           mimutes of zeros in a row (non-were time)
              - TOL:               tolerance (in minutes)
    """
    df.cache()
    
    cols = F.split(df[data_col], r',')
    
    app_fun = F.udf(lambda x: activity_index(x, interval))
    
    # use the Axis1 acceleration to determine the activity count
    df = df.withColumn('activity', cols.getItem(0).cast(IntegerType()))
    
    df = df.withColumn('activityIntensity', app_fun(df['activity'])
                      ).drop(data_col).orderBy(datetime_col)
    
    # Determine periods when the accelerometer is not used
    
    return df

##########################################################################################################

def activity_index(AC, interval, LightCO = 100, ModerateCO = 1953, HardCO = 5725, VeryHardCO = 9999):
         
    """
        Calculates activity intensity level using Freedson adult cut points 
        (Freedson, Melanson, & Sirard, 1998)
        
        Input: 
                - AC:                                   activity count per epoch
                - interval:                             duration of a single epoch (in seconds)
                - LightCO...VeryHardCO:                 activity count cutoffs per minute
                
            
    """
    
    # assume epoch smaller than 1 minute
    assert interval < 60, "Epoch larger than 1 minute."
    
    # normalize the cutoffs per epoch
    n = 60/interval 
    VeryHardCO = VeryHardCO/n
    HardCO = HardCO/n
    ModerateCO = ModerateCO/n
    LightCO = LightCO/n
    
    if AC == -1:
        act_index = -1                          # state unknown
    elif AC == -2:
        act_index = -2                          # not wearing device
    elif 0 <= AC < LightCO:
        act_index = 0                           # sedentary
    elif LightCO <= AC < ModerateCO:
        act_index = 1                           # light activity
    elif ModerateCO <= AC < HardCO:
        act_index = 2                           # moderate activity
    elif HardCO <= AC < VeryHardCO:
        act_index = 3                           # hard activity
    else:
        act_index = 4                           # very hard activity
    
    return act_index

##########################################################################################################

def datetime_filter(df, param_name, param_value, datetime_name, time_w=90, step=90*60):
    """
        Remove rows in dataframe which match a condition within a time window
        
        Input:
                df:                    Spark dataframe object containing accelerometer data
                param_name:            parameter name
                param_value:           parameter value value for conditional statement
                datetime_name:         name of column containning timestamps
                time_w:                tumbling window duration (in minutes)
                step:                  sliding interval (in seconds)
    
    """
    
    
    # Tumbling window size
    tw = str(time_w) + ' minutes'
    
    # Sliding window size
    sw = str(step) + ' seconds'
    
    # offset (in seconds)
    offset = str(0) + ' seconds'
    
    
    intervals_df = df.groupBy(
                F.window(datetime_name, '{}'.format(tw),'{}'.format(sw),'{}'.format(offset))
             ).avg(param_name)\
              .sort('window.start')\
              .filter(F.col('avg({})'.format(param_name))==param_value)\
              .select('window')\
              .withColumn('start', F.col('window').start)\
              .withColumn('end', F.col('window').end)\
              .drop('window')
           
    """
        schema of internal_df:
        
        root
         |-- start: timestamp (nullable = true)
         |-- end: timestamp (nullable = true)
    """
    
    # Transform dataframe into list of pyspark.sql.types.Row objects
    intervals_list = intervals_df.collect()
    
    # filter dataframe excluding the selected intervals
    for row in intervals_list:
        df = df.filter(~F.col(datetime_name).between(row[0],row[1]))
        
    return intervals_df, df

##########################################################################################################

def start_time_offset(df):
    """
        Return the offset to start a tumbling window from the first timestamp of a dataframe df
    """
    
    st_date = df.first()[0]
    st_min = st_date.minute
    st_sec = st_date.second
    start_time = (st_min-10*(st_min//10))*60 + st_sec
    offset = '{} seconds'.format(str(start_time))
    
    return offset

##########################################################################################################

def consecutive_time(df, ts_name, interval):
    """
        Add two column to the dataframe with the start date and end date of consecutive timestamps
        which differ by a given interval
        
        Input:
        
                - df:                    Spark dataframe object containing timestamps data
                - ts_name:               name of column with timestamps
                - interval:              required precision (in seconds)
    
    """
    
    df_ = df.withColumn("rn", F.row_number().over(Window.orderBy('{}'.format(ts_name))))
    df_.cache()
    df_.createOrReplaceTempView('df_')
    
    spark  = SparkSession.builder.getOrCreate()
    
    df_ = spark.sql(""" WITH tmp AS(
                              SELECT *, BIGINT({}) - rn * {} AS totsec
                              FROM df_)
                        SELECT  *, MIN({}) OVER(PARTITION BY totsec) AS start, 
                                   MAX({}) OVER(PARTITION BY totsec) AS end,
                                   ROW_NUMBER() OVER(PARTITION BY totsec ORDER BY {}) AS id
                        FROM tmp
                    """.format(ts_name, str(interval), ts_name, ts_name, ts_name)).drop('totsec').drop('rn')
    df_.createOrReplaceTempView('df_')
    df_ = spark.sql(""" SELECT *, BIGINT(start) - LAG(BIGINT(end),1,BIGINT(end)) OVER(ORDER BY timestamp) AS pause
                        FROM df_
                    """)
    
    return df_

##########################################################################################################

def detect_bouts(df, ts_name, col_name, new_col, interval, UP=9999, LOW=1953, DURATION=10, TOL=2):
    """
        Determine a new column based on filters on timestamps
        
        Input:
                - df:                    Spark dataframe object containing timestamps data
                - ts_name:               name of column with timestamps
                - col_name:              name of column on which the filter must bbe applied
                - new_col:               name of new column where the filters are applied
                - interval:              epoch period (in seconds)
                - UP:                    upper limit of activity count per minute
                - LOW:                   lower limit of activity count per minute
                - DURATION:              minimum bout duration (in minutes)
                - TOL:                   tolerance (in minutes)
     
    """
    
    # Assume one epoch smaller than 1 minute
    assert interval < 60, "Epoch larger than 1 minute."
    
    # Number of epochs per minute
    n = 60/interval
    
    # bounds on measured quantitity per epoch
    up, low = (UP/n, LOW/n)
    
    # Convert tolerance in seconds
    tol = TOL*60
    
    # Number of epoch in tolerance interval
    epochs_tol = TOL*60/interval
    
    # Convert minimum bout duration in seconds
    duration = DURATION*60
    
    # Number of epochs in minimum bout duration
    epochs_min_bout = duration/interval
    
    # Filter dataframe with:   low <= col_name <= up
    inbout = (F.col('{}'.format(col_name)) >= low) & (F.col('{}'.format(col_name)) <= up)
    df1 = df.filter(inbout).orderBy('{}'.format(ts_name))
    df1.cache()
    
    # Determine consecutive timestamps in df1
    df1 = consecutive_time(df1, '{}'.format(ts_name), interval)
    df1 = df1.selectExpr(['{}'.format(ts_name),'start as activity_start'])
    
    if TOL > 0:
        # Filter data with col_name < low and col_name > up
        df2 = df.filter(~inbout).orderBy('{}'.format(ts_name))
        df2.cache()

        # Determine consecutive timestamps in df2
        df2 = consecutive_time(df2, '{}'.format(ts_name), interval)
        df2 = df2.selectExpr(['{}'.format(ts_name),'start as pause_start'])
    
        # Filter periods larger than tolerance
        df2 = df2.groupBy('pause_start').count()
        df2 = df2.filter(df2['count'] > epochs_tol).orderBy('pause_start')
        df2 = df2.withColumn('pause_end', (F.col('pause_start').cast(IntegerType()) +\
                                          (F.col('count')-1)*interval).cast(TimestampType())
                            ).drop('count')
    
        pause_list = df2.collect()
        
    # Merge df1 to the accelerometer dataframe
    df3 = df.join(df1, ['{}'.format(ts_name)], 'leftouter')
    df3.cache()
    
    if TOL > 0:
        # Assign pause periods
        df3 = df3.withColumn('pause', F.lit(0))
        for row in pause_list:
            df3 = df3.withColumn('pause', F.when((F.col('{}'.format(ts_name)) >= row['pause_start']) &\
                                                 (F.col('{}'.format(ts_name)) <= row['pause_end']),
                                                 1
                                                )
                                           .otherwise(F.col('pause'))
                                )
    
        # Assign previous non-zero 'start' to missing values given 'pause' < tolerance
        df3 = df3.withColumn('activity_start', F.when((F.col('activity_start').isNull()) &\
                                                      (F.col('pause') == 0), 
                                                      F.last(F.col('activity_start'), ignorenulls=True)
                                                       .over(Window.orderBy('timestamp'))
                                                     )
                                                .otherwise(F.col('activity_start'))
                            ).drop('pause')
    
    # Define a flag to select rows with non-zero 'activity_start'
    df3 = df3.withColumn('check', F.when(F.col('activity_start').isNotNull(), F.lit(1))\
                                   .otherwise(F.lit(0))
                        )
    
    # Select rows with non-zero 'activity_start'
    df2 = df3.select(['{}'.format(ts_name), 'check']).filter(F.col('check') == 1)
    df2.cache()
    
    # Assign bout start
    df2 = consecutive_time(df2,'{}'.format(ts_name), interval)\
          .selectExpr(['{}'.format(ts_name),'start as bout_start'])
    
    # Assign bout to dataframe 
    df3 = df3.join(df2, ['{}'.format(ts_name)], 'leftouter').drop(*['activity_start','check'])
    df3 = df3.withColumn('bout_start', F.when(F.col('bout_start').isNull(), F.col('timestamp'))\
                                        .otherwise(F.col('bout_start'))                        
                        )
    
    # Filter periods larger than the minimum bout duration
    df1 = df3.groupBy('bout_start').count()
    df1.cache()
    df1 = df1.filter(df1['count'] > epochs_min_bout).orderBy('bout_start')
    df1 = df1.withColumn('bout_end', (F.col('bout_start').cast(IntegerType()) +\
                                      F.col('count')*interval).cast(TimestampType())
                        ).drop('count')
    df1 = df1.withColumn(new_col, F.row_number().over(Window.orderBy('bout_start')))
    
    bouts_list = df1.collect()
    
    # Initialize activityBoutNumber to zero
    df3 = df3.drop(*['start','end','check','pause','bout_start'])
    df3 = df3.withColumn(new_col, F.lit(0))
    
    # Assign activityBoutNumber
    for row in bouts_list:
        df3 = df3.withColumn(new_col, F.when((F.col('{}'.format(ts_name)) >= row['bout_start']) &\
                                             (F.col('{}'.format(ts_name)) <= row['bout_end']),
                                             row[new_col]
                                            )
                                       .otherwise(F.col(new_col))
                            )
    df3 = df3.orderBy('{}'.format(ts_name))
    
    return df3

##########################################################################################################

def non_wear_filter(df, ts_name, AC_name, AI_name, interval, UP=0, LOW=0, DURATION=90):
    
    """
    
    
    """
    
    # Select valid epochs with non-negative activity count
    df1 = df.filter(F.col(AC_name) >= 0)
    df1.cache()
    TOL = 0
    new_col = 'no_wear'
    df1 = detect_bouts(df, ts_name, AC_name, new_col, interval, UP, LOW, DURATION, TOL)
    df1 = df1.select([ts_name, new_col])
    
    # Merge new column with the dataframe and assing zero to missing values
    df2 = df.join(df1, [ts_name], 'leftouter').orderBy(ts_name).fillna(0, subset=[new_col])
    df2.cache()
    
    # Assign activity count and activity intensity equal to -2 for non valid data
    df2 = df2.withColumn(AC_name, F.when(F.col(new_col)>0, -2).otherwise(F.col(AC_name)))
    df2 = df2.withColumn(AI_name, F.when(F.col(new_col)>0, -2).otherwise(F.col(AI_name))).drop(new_col)
    
    return df2

##########################################################################################################    
    
def activity_bout_filter(df, ts_name, AC_name, new_col, interval, UP=9999, LOW=1953, DURATION=10, TOL=2):
    """
    
    """
    
    # Select valid epochs with non-negative activity count
    df1 = df.filter(F.col(AC_name) >= 0)
    df1.cache()
    df1 = detect_bouts(df, ts_name, AC_name, new_col, interval, UP, LOW, DURATION, TOL)
    df1 = df1.select([ts_name, new_col])
    
    # Merge new column with the dataframe and assing zero to missing values
    df2 = df.join(df1, [ts_name], 'leftouter').orderBy(ts_name).fillna(0, subset=[new_col])
    df2.cache()
    
    return df2

##########################################################################################################

def sedentary_bout_filter(df, ts_name, AC_name, new_col, interval, UP=180, LOW=0, DURATION=30, TOL=1):
    """
    
    
    """
    # Select valid epochs with non-negative activity count
    df1 = df.filter(F.col(AC_name) >= 0)
    df1.cache()
    df1 = detect_bouts(df, ts_name, AC_name, new_col, interval, UP, LOW, DURATION, TOL)
    df1 = df1.select([ts_name, new_col])
    
    # Merge new column with the dataframe and assing zero to missing values
    df2 = df.join(df1, [ts_name], 'leftouter').orderBy(ts_name).fillna(0, subset=[new_col])
    df2.cache()
    
    return df2

##########################################################################################################

def round_timestamp(df, ts_name, interval=5):
    """
        Round of the timestamps in the dataframe according to a given precision
        
        Input: 
                - df:                    Spark dataframe object containing timestamps data
                - ts_name:               name of column with timestamps
                - interval:              required precision (in seconds)

    """
    
    # NOTICE: within "F.col("seconds")/interval)*interval-interval" we subtract "interval" 
    #         to match the PALMS output
    
    data = df.withColumn("seconds", F.second(ts_name))\
    .withColumn("round_seconds", F.round(F.col("seconds")/interval)*interval-interval)\
    .withColumn("add_seconds", (F.col("round_seconds") - F.col("seconds")))\
    .withColumn("timestamp_sec", F.col(ts_name).cast("long"))\
    .withColumn("tot_seconds", F.col("timestamp_sec") + F.col("add_seconds"))\
    .drop("add_seconds").drop('timestamp_sec').drop('seconds').drop('round_seconds')\
    .withColumn(ts_name, F.col("tot_seconds")\
                .cast(dataType=TimestampType())
               )\
    .drop("tot_seconds")
    
    return data

##########################################################################################################

def fill_timestamp(df, ts_name, interval=5, ws=600):
    """
        Fill in missing timestamps with the last available data up to a given time range
        
        Input:
                - df:                    Spark dataframe object containing timestamps data
                - ts_name:               name of column with timestamps
                - interval:              difference between consecutive timestamps (in seconds)
                - ws:                    window size (in seconds)
                - spark:                 SparkSession object        
                
    """
    
    minp, maxp = df.select(
        F.min(ts_name).cast('long'), 
        F.max(ts_name).cast('long')
    ).first()
    
    spark  = SparkSession.builder.getOrCreate()
    
    ref = spark.range(minp, maxp, interval).select(F.col('id').cast('timestamp').alias(ts_name))
    ref.cache()
    ref = ref.join(df, [ts_name], how='leftouter')
    
    ref = ref.withColumn('total_sec', F.col(ts_name).cast('long'))
    
    for item in df.columns:
        ref = ref.withColumn(item, F.when(ref[item].isNotNull(),ref[item])\
                            .otherwise(F.last(ref[item], ignorenulls=True)\
                                      .over(Window.orderBy('total_sec')\
                                           .rangeBetween(-ws,0))))
    ref = ref.drop('total_sec').dropna()
    
    return ref

##########################################################################################################

def filter_speed(df, vcol, vmax=130):
    """
        Exclude data with velocity larger that give value
        
        Input:
                - df:                    Spark dataframe object containing timestamps data
                - vcol:                  name of column with speed measurements
                - vmax:                  speed cutoff (km/h)
        
    """
    
    return df.filter(df['{}'.format(vcol)] <= vmax)

##########################################################################################################

def filter_height(df, hcol, tscol, dhmax=1000):
    """
        Excludes data points where the change of height between two consecutive points is larger dhmax
        
        Input:
                - df:                    Spark dataframe object containing timestamps data
                - hcol:                  name of column with height measurements
                - tscol:                 name of column with timestamps
                - dhmax:                 height change cutoff (meters)
        
    """
    
    spark  = SparkSession.builder.getOrCreate()
    
    df.createOrReplaceTempView('df')
    ref = spark.sql("""select *, {} - lag({}, 1, 0)
                            OVER (ORDER by {}) AS d_height
                            FROM df""".format(hcol,hcol,tscol))
    
    # if the first value of the height is zero, then the second row may be removed
    ref = ref.filter((ref['d_height']<=dhmax)).drop('d_height')
    
    # if the first value of the height is zero, this line inserts the second row back into the dataframe
    ref = ref.union(df.limit(2)).orderBy(tscol).drop_duplicates()
    
    return ref

##########################################################################################################

