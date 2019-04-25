from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
from math import *
import numpy as np

##########################################################################################################

def gen_gps_dataframe(df, datetime_format, id):
    """
        GENERATE GPS DATAFRAME
        
        Input parameters:
                - df:                      the Spark dataframe object containing the GPS raw data
                - datetime_format:         the datetime format of raw data
                - id:                      dataset id
        
    """
    
    def utc_to_local(utc_dt, lat, lon):
    
        from tzwhere import tzwhere
        import pytz
    
        tzwhere = tzwhere.tzwhere()
        timezone_str = tzwhere.tzNameAt(lat, lon) 
        
        time_zone = pytz.timezone(timezone_str)
        utcoff = time_zone.utcoffset(utc_dt)
    
        return utcoff.total_seconds() 
    
    def convert_speed(speed):
        
        speed_ = str(speed).replace(' km/h', '')
        
        return speed_
    
    def convert_height(height):
        
        height_ = str(height).replace(' M', '')
        
        return height_
    
    def remove_space(col):
        
        col_ = str(col).replace(' ', '')
        
        return col_
    
    utc_to_local_fun = F.udf(utc_to_local)
    convert_speed_fun = F.udf(convert_speed)
    convert_height_fun = F.udf(convert_height)
    remove_space_fun = F.udf(remove_space)
    
    # Convert column names to lowercase and remove spaces
    # Rename the colums for the speed and the height
    cols = df.columns
    cols = [c.lower() for c in cols]
    cols = [col.replace('(km/h)' , '') for col in cols]
    cols = [col.replace('(m)' , '') for col in cols]
    cols = [c.strip().replace(' ', '_') for c in cols]
    cols = [col.replace('n_s' , 'n/s') for col in cols]
    cols = [col.replace('e_w', 'e/w') for col in cols]

    gps_data = df.toDF(*cols)
    gps_data.cache()
    
    # Drop duplicates
    gps_data = gps_data.drop_duplicates() #TODO: implement better this condition: avoid to drop valid points
    
    header = gps_data.columns 
    
    # Rename third and fourth columns, which define date and time, respectively
    gps_data = gps_data.withColumn("date", F.col(header[2]))
    gps_data = gps_data.drop(header[2])
    gps_data = gps_data.withColumn("time", F.col(header[3]))
    gps_data = gps_data.drop(header[3])
    
    # Define latitude and longitude with sign and remove duplicated columns
    gps_data = gps_data.withColumn('lat', 
                                   F.when(remove_space_fun(F.col('n/s')) == 'S', 
                                          F.col('latitude')*(-1)
                                         ).otherwise(F.col('latitude'))
                                  ).drop('latitude').drop('n/s')
    
    gps_data = gps_data.withColumn('lon', 
                                   F.when(remove_space_fun(F.col('e/w')) == 'W', 
                                          F.col('longitude')*(-1)
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
    
    # Initialize the distance
    gps_data = gps_data.withColumn('distance', F.lit(0.0))
    
    # Convert speeds  in units of km/h
    gps_data = gps_data.withColumn('speed', convert_speed_fun(F.col('speed')).cast(DoubleType()))
    
    # Convert heights in units of meter
    gps_data = gps_data.withColumn('height', convert_height_fun(F.col('height')).cast(DoubleType()))
    
    # Calculate the offset with respect the UTC time
    sample = gps_data.sample(False, 0.1, seed=0).limit(10).select('timestamp','lat','lon')
    sample = sample.withColumn('ts', utc_to_local_fun(F.col('timestamp'), F.col('lat'), F.col('lon')).cast(DoubleType()))
    sample = sample.select('ts').collect()
    sample = [int(row.ts/3600.0) for row in sample]
    
    ## get the offset looking at the most frequent time interval in the list sample
    offset = max(set(sample), key=sample.count)
    
    # Apply time offset
    gps_data = gps_data.withColumn('timestamp', 
                                   gps_data.timestamp + F.expr('INTERVAL {} HOURS'.format(str(offset)))
                                  )
    
    # Calculate day of the week
    gps_data = gps_data.withColumn('dow',
                                   F.date_format('timestamp','u')
                                   )
    
    drop_list = ['date', 'time']
    
    gps_data = gps_data.withColumn('ID', F.lit(id)).orderBy('timestamp')
    
    gps_data = gps_data.select('ID','timestamp', 'dow', 'lat', 'lon', 
                               'distance', 'height', 'speed').orderBy('timestamp')
    
    return gps_data

##########################################################################################################

def round_timestamp(df, ts_name, interval):
    """
        Round of the timestamps in the dataframe according to a given precision
        
        Input: 
                - df:                    Spark dataframe object containing timestamps data
                - ts_name:               name of column with timestamps
                - interval:              required precision (in seconds)

    """
    
    # Notice: use floor() instead of round() to match PALMS output
    
    data = df.withColumn("seconds", F.second(ts_name))\
    .withColumn("round_seconds", F.floor(F.col("seconds")/interval)*interval)\
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

def calc_distance(lat1, lon1, lat2, lon2):
    
    """
        Implementation of the haversine formula
    """
    
    R = 6367000.0 # Earth average radius used by PALMS 

    lat1R = radians(lat1)
    lon1R = radians(lon1)
    lat2R = radians(lat2)
    lon2R = radians(lon2)

    dlon = lon2R - lon1R
    dlat = lat2R - lat1R
    a = (sin(dlat/2))**2 + cos(lat1R) * cos(lat2R) * (sin(dlon/2))**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    distance = R * c
    
    return distance


def haversine_dist(lat1, lon1, lat2, lon2):

    """
        Implementation of the haversine formula
    """
    # Earth radius (km)
    R = 6367 
    
    lat1, lon1, lat2, lon2 = map(np.deg2rad, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1 
    dlon = lon2 - lon1 
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    dist = 2 * np.arcsin(np.sqrt(a)) * R
    
    return dist

##########################################################################################################

def set_fix_type(df, ts_name, ws):
    """
        Set fix type
        
        Input:
                - df:                    Spark dataframe object containing timestamps data
                - ts_name:               name of column with timestamps     
                - ws:                    signal loss window size (in seconds)
    
        fixTypeCode - integer value representing type of GPS fix:

               -1 = unknown
                0 = invalid
                1 = valid (raw, unprocessed)
                2 = first fix
                3 = last fix
                4 = last valid fix
                5 = lone fix
                6 = inserted fix 
         
    """
    minp = df.select(F.min(ts_name).cast('long')).first()[0]
    
    w = Window.orderBy(ts_name)
    
    df2 = df.withColumn('total_sec', F.col(ts_name).cast('long'))
    df2.cache()
    
    # Set fix type of first row as 'first fix'
    df2 = df2.withColumn('fixTypeCode', F.when(F.col('total_sec') == minp, 2))
                        
    # Set a new 'first fix' when the interval between current and previous timestamps 
    # is larger than the GPS loss signal duration, otherwise it is a 'valid' fix
    df2 = df2.withColumn('fixTypeCode', 
                         F.when(F.col('total_sec') - F.lag('total_sec',1,0)
                                                      .over(w) > ws, 2)
                         .otherwise(1)
                        )
        
    # Set 'lone fix' when the current fix and the following are both set to 'first fix'
    df2 = df2.withColumn('fixTypeCode', 
                         F.when(F.col('fixTypeCode') + F.lead('fixTypeCode')
                                                       .over(w) == 4, 5)
                          .otherwise(F.col('fixTypeCode'))
                        )
    
    # TODO: filter out 'lone fixes'
   
    # Set 'last fix'
    df2 = df2.withColumn('fixTypeCode', 
                         F.when(F.lead('fixTypeCode').over(w) == 2, 3)
                          .otherwise(F.col('fixTypeCode'))
                        ).drop('total_sec')
    
    df2.select(df.columns + ['fixTypeCode'])
    
    return df2

##########################################################################################################

def fill_timestamp(df, ts_name, fix_type_name, interval, ws):
    """
        Fill in missing timestamps with the last available data up to a given time range
        
        Input:
                - df:                    Spark dataframe object containing timestamps data
                - ts_name:               name of column with timestamps
                - dist_name:             name of column with distance measurements
                - fix_type_name:         name of column with fix type code
                - interval:              difference between consecutive timestamps (in seconds)
                - ws:                    signal loss window size (in seconds)
               
    """
    
    minp, maxp = df.select(
        F.min(ts_name).cast('long'), 
        F.max(ts_name).cast('long')
    ).first()
    

    spark  = SparkSession.builder.getOrCreate()
    
    w1 = Window.orderBy('total_sec').rangeBetween(-ws-interval,0)
    w2 = Window.orderBy('total_sec')
    
    ref = spark.range(minp, maxp, interval).select(F.col('id').cast(TimestampType()).alias(ts_name))
    ref.cache()
    
    ref = ref.join(df, [ts_name], how='leftouter')
    ref = ref.withColumn('total_sec', F.col(ts_name).cast('long'))
    
    columns = df.columns
    columns.remove(fix_type_name)
    
    for item in columns:
        ref = ref.withColumn(item, F.when(ref[item].isNotNull(),ref[item])
                                    .otherwise(F.last(ref[item], ignorenulls=True)
                                                .over(w1))
                            )

    # Set 'last valid fix'
    ref = ref.withColumn(fix_type_name, F.when((F.col(fix_type_name).isNull()) &
                                               (F.last(fix_type_name, ignorenulls=True)
                                                 .over(w2) == 3), 4)
                                         .otherwise(F.col(fix_type_name))
                        )
    
    # Set 'inserted fix'
    ref = ref.withColumn(fix_type_name, F.when(F.col(fix_type_name).isNull(), 6)
                                         .otherwise(F.col(fix_type_name))
                        )
                
    ref = ref.drop('total_sec').dropna()
    
    cols = ref.columns
    cols.remove('ID')
    cols.insert(0,'ID')

    ref = ref.select(cols).orderBy(ts_name)
    
    return ref

##########################################################################################################

def filter_speed(df, vcol, vmax):
    """
        Exclude data with velocity larger that give value
        
        Input:
                - df:                    Spark dataframe object containing timestamps data
                - vcol:                  name of column with speed measurements
                - vmax:                  speed cutoff (km/h)
        
    """
    
    cond = (F.col('fixTypeCode') == 1)
    
    df = df.withColumn('check', F.when(cond &
                                       (F.col(vcol) > vmax),
                                       1)
                      )
    
    df = df.filter(F.col('check').isNull())
    df = df.drop('check')
    
    return df
    
##########################################################################################################

def filter_height(df, hcol, tscol, dhmax):
    """
        Excludes data points where the change of height between two consecutive points is larger dhmax
        
        Input:
                - df:                    Spark dataframe object containing timestamps data
                - hcol:                  name of column with height measurements
                - tscol:                 name of column with timestamps
                - dhmax:                 height change cutoff (meters)
        
    """
    
    cond = (F.col('fixTypeCode') == 1)
    
    spark  = SparkSession.builder.getOrCreate()
    
    df.createOrReplaceTempView('df')
    ref = spark.sql("""select *, {} - lag({}, 1, 0)
                            OVER (ORDER by {}) AS d_height
                            FROM df""".format(hcol,hcol,tscol))
    
    # if the first value of the height is zero, then the second row may be removed
    ref = ref.withColumn('check', F.when(cond &
                                         (F.abs(ref['d_height']) > dhmax),
                                         1)
                      )
    
    ref = ref.filter(F.col('check').isNull())
    ref = ref.drop(*['check','d_height'])
    
    # if the first value of the height is zero, this line inserts the second row back into the dataframe
    ref = ref.union(df.limit(2)).orderBy(tscol).drop_duplicates()
    
    return ref

##########################################################################################################

def filter_change_dist_3_fixes(df, dcol, tscol, dmin):
    """
        Excludes data points where the minimum change in distances between three fixes is less than dmin
        
        Input:
                - df:                    Spark dataframe object containing timestamps data
                - dcol:                  name of column with distance measurements
                - tscol:                 name of column with timestamps
                - dmin:                  minimum distance over three fixes
        
        NOTICE: casting latitude and longitude to double precision will result in more points filtered out.
    """
    
    # Calculate distance between two fixes
    app_fun = F.udf(lambda a,b,c,d: calc_distance(a,b,c,d))
    
    # Define a window over timestamps
    w = Window.orderBy(tscol)
    
    cond = (F.col('fixTypeCode') == 1)
    
    first_lat = df.select('lat').first()[0]
    first_lon = df.select('lon').first()[0]
    last_lat = df.select(F.last('lat')).first()[0]
    last_lon = df.select(F.last('lon')).first()[0]
    
    lat0 = F.col('lat').cast(DoubleType())
    lon0 = F.col('lon').cast(DoubleType())
    lat1 = F.lead(F.col('lat'),1,last_lat).over(w)
    lon1 = F.lead(F.col('lon'),1,last_lon).over(w)
    lat2 = F.lag(F.col('lat'),1,first_lat).over(w)
    lon2 = F.lag(F.col('lon'),1,first_lon).over(w)
    
    # Recaulculate the traveled from last fix
    df2 = df.withColumn(dcol, F.when((F.col('lat') != first_lat) & (F.col('lat') != last_lat),
                                            app_fun(lat0,lon0,lat2,lon2)
                                          )
                                     .otherwise(F.col(dcol))
                        )
    
    # Calculate distance between next fix and previous fix
    df2 = df2.withColumn('dist', F.when((F.col('lat') != first_lat) & (F.col('lat') != last_lat),
                                        app_fun(lat1,lon1,lat2,lon2)
                                       )
                        )
    
    condition = (F.col(dcol) > dmin) & (F.col('dist').isNotNull()) & (F.col('dist') < dmin)
    
    df2 = df2.withColumn('check', F.when(cond &
                                         condition,
                                         1)
                       )
    
    df2 = df2.filter(F.col('check').isNull())
    df2 = df2.drop('check')
    
    df2 = df2.drop('dist')
    
    return df2

##########################################################################################################

def filter_acceleration(df, scol, tscol, amax=7):
    """
        Excludes data points where the acceleration is more than amax.
        
        amax = 7 m/s^2 is equivalent to a Ferrari, typical car 3-4
        
        Input:
                - df:                    Spark dataframe object containing timestamps data
                - scol:                  name of column with speed measurements
                - tscol:                 name of column with timestamps
                - amax:                  maximum acceleration
        
    """
    
    # Define a window over timestamps
    w = Window.orderBy(tscol)
    
    cond = (F.col('fixTypeCode') == 1)
    
    # Calculate acceleration
    df2 = df.withColumn('acc', F.abs((F.col(scol) - F.lag(F.col(scol),1).over(w)))/
                               (F.col(tscol).cast(IntegerType()) -
                                F.lag(F.col(tscol),1).over(w).cast(IntegerType()))*1000/3600
                        )
    
    condition = (F.col('acc').isNotNull()) & (F.col('acc') >= amax)
    
    df2 = df2.withColumn('check', F.when(cond &
                                         condition,
                                         1)
                       )
    
    df2 = df2.filter(F.col('check').isNull())
    df2 = df2.drop('check')
    
    
    df2 = df2.drop('acc')
    
    return df2

##########################################################################################################

def select_gps_intervals(df, ts_name, window):
    """
    
    """
    minp = df.select(F.min(ts_name).cast('long')).first()[0]
    
    df2 = df.withColumn('tmp', F.row_number().over(Window.orderBy(ts_name))-1)

    #df2 = df2.withColumn('tmp', F.row_number().over(Window.orderBy(ts_name)))

    df2 = df2.withColumn('total_sec', F.col(ts_name).cast('long'))
    
    df2 = df2.withColumn('duration', F.col(ts_name).cast(IntegerType())-
                                     F.lag(F.col(ts_name).cast(IntegerType()),1,minp)
                                      .over(Window.orderBy(ts_name))
                    ).drop('total_sec')
    
    df2 = df2.withColumn('tmp', (F.col('tmp')*F.col('duration'))%window).drop('duration').orderBy(ts_name)
    
    df2 = df2.filter(F.col('tmp') == 0).drop('tmp').orderBy(ts_name)
    
    return df2

##########################################################################################################
    
def proc_segment(df, index, ts_name, min_dist_per_min, min_pause_duration, max_pause_time, state, trip, FLAG):
    """
        set: 3 3 duration 0.0
    """
    
    if (index == 'i1') | (index == 'j1'):
        
        cond = F.col(index).isNotNull() & (F.col('pause_dist') >= min_dist_per_min) &\
               (F.col('pause') < min_pause_duration)
        
    elif (index == 'i2') | (index == 'j2'):
        
        cond = F.col(index).isNotNull() & (F.col('pause_dist') >= min_dist_per_min) &\
               (F.col('pause') >= min_pause_duration) & (F.col('pause') <= max_pause_time)
        
    elif (index == 'i3') | (index == 'j3'):
        
        cond = F.col(index).isNotNull() & (F.col('segment') != 0) & (F.col('pause') > max_pause_time) 
    
    elif (index == 'j4'):
        
        # sanity check: this is the case when the end of the segment has pause <= max_pause_time and
        #               pause_dist < min_dist_per_min
        w = Window.orderBy(ts_name)
        cond = F.col(index).isNotNull() & (F.col('pause_dist') < min_dist_per_min) &\
               (F.col('pause') <= max_pause_time)
        
    else:
        
        print('index = {} is not allowed.'.format(index))
        exit()
    
    w2 = Window.partitionBy('segment').orderBy(ts_name).rowsBetween(Window.unboundedPreceding, 0)
    
    df = df.withColumn('check', F.when(F.col(index).isNotNull(), F.lit(0)))
        
    df = df.withColumn('pause2', F.when(cond, F.col('pause')))
        
    df = df.withColumn('pause2', F.when(F.col(index).isNotNull() &
                                        F.col('pause2').isNull(),
                                        F.last('pause2', ignorenulls=True).over(w2)
                                        ).otherwise(F.col('pause2'))
                      ).orderBy(ts_name)
        
    df = df.withColumn('state', F.when(F.col(index).isNotNull() &
                                       F.col('pause2').isNull(),
                                       state
                                       ).otherwise(F.col('state'))
                      )
        
    df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                          F.col('pause2').isNull(),
                                          trip
                                          ).otherwise(F.col('tripType'))
                      )
        
    if (FLAG == 'CASE1') | (FLAG == 'CASE2'):
            
        w2 = Window.partitionBy('segment').orderBy(ts_name)
            
        df = df.withColumn('state', F.when(F.col(index).isNotNull() &
                                           F.col('pause2').isNotNull() &
                                           F.lag('pause2',1).over(w2).isNull(),
                                           2
                                          ).otherwise(F.col('state'))
                          ).orderBy(ts_name)
            
        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              F.col('pause2').isNotNull() &
                                              F.lag('pause2',1).over(w2).isNull(),
                                              2
                                             ).otherwise(F.col('tripType'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('check2', F.when(F.col(index).isNotNull() &
                                            (F.col('cum_dist_min') >= min_dist_per_min) &
                                            (F.lag('state', 1).over(w2) == 2) &
                                            (F.lag('tripType', 1).over(w2) == 2) &
                                            F.lag('pause2', 2).over(w2).isNull(),
                                            0)
                          ).orderBy(ts_name)
        
        df = df.withColumn('check2', F.when(F.col(index).isNotNull() &
                                            (F.lag('state', 1).over(w2) == 3) &
                                            (F.lag('tripType', 1).over(w2) == 2) &
                                            (F.col('state') == 3) &
                                            (F.col('tripType') == 3) &
                                            (F.col('pause') != F.col('duration')),
                                            1).otherwise(F.col('check2'))      
                          ).orderBy(ts_name)
        
        df = df.withColumn('check2', F.when(F.col(index).isNotNull() &
                                            F.col('check2').isNull(),
                                            F.last('check2', ignorenulls = True).over(w2)
                                           ).otherwise(F.col('check2'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('pause', F.when(F.col(index).isNotNull() &
                                           (F.col('check2')  == 1) &
                                           (F.lag('check2', 1).over(w2)  == 0) &
                                           (F.lag('state', 1).over(w2) == 3) &
                                           (F.lag('tripType', 1).over(w2) == 2) &
                                           (F.col('state') == 3) &
                                           (F.col('tripType') == 3) &
                                           (F.col('pause') != F.col('duration')),
                                           F.col('duration')).otherwise(F.col('pause'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('pause_dist', F.when(F.col(index).isNotNull() &
                                                (F.col('check2')  == 1) &
                                                (F.lag('check2', 1).over(w2)  == 0) &
                                                (F.lag('state', 1).over(w2) == 3) &
                                                (F.lag('tripType', 1).over(w2) == 2) &
                                                (F.col('state') == 3) &
                                                (F.col('tripType') == 3) &
                                                (F.col('pause') == F.col('duration')),
                                                0.0).otherwise(F.col('pause_dist'))
                          ).orderBy(ts_name)
            
        df = df.withColumn('state', F.when(F.col(index).isNotNull() &
                                           (F.col('cum_dist_min') < min_dist_per_min) &
                                           (F.lag('state', 1).over(w2) == 2) &
                                           (F.lag('tripType', 1).over(w2) == 2) &
                                           F.lag('pause2', 2).over(w2).isNull(),
                                           3).otherwise(F.col('state'))
                          ).orderBy(ts_name)
            
        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              (F.col('cum_dist_min') < min_dist_per_min) &
                                              (F.lag('state', 1).over(w2) == 2) &
                                              (F.lag('tripType', 1).over(w2) == 2) &
                                              F.lag('pause2', 2).over(w2).isNull(),
                                              2).otherwise(F.col('tripType'))
                          ).orderBy(ts_name)
            
        df = df.withColumn('state', F.when(F.col(index).isNotNull() &
                                           (F.lag('state', 1).over(w2) == 3) &
                                           (F.lag('tripType', 1).over(w2) == 2) &
                                           F.lag('pause2', 3).over(w2).isNull() &
                                           F.lag('pause2', 2).over(w2).isNotNull(),
                                           3).otherwise(F.col('state'))
                          ).orderBy(ts_name)
            
        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              (F.lag('state', 1).over(w2) == 3) &
                                              (F.lag('tripType', 1).over(w2) == 2) &
                                              F.lag('pause2', 3).over(w2).isNull() &
                                              F.lag('pause2', 2).over(w2).isNotNull(),
                                              3).otherwise(F.col('tripType'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('check', F.when(F.col(index).isNotNull() &
                                              (F.lag('state', 1).over(w2) == 3) &
                                              (F.lag('tripType', 1).over(w2) == 2) &
                                              F.lag('pause2', 3).over(w2).isNull() &
                                              F.lag('pause2', 2).over(w2).isNotNull(),
                                              1).otherwise(F.col('check'))
                          ).orderBy(ts_name)
            
        df = df.withColumn('pause', F.when(F.col(index).isNotNull() &
                                           (F.col('check')  == 1) &
                                           (F.lag('state', 1).over(w2) == 3) &
                                           (F.lag('tripType', 1).over(w2) == 2) &
                                           (F.col('state') == 3) &
                                           (F.col('tripType') == 3) &
                                           (F.col('pause') != F.col('duration')),
                                           F.col('duration')).otherwise(F.col('pause'))
                          ).orderBy(ts_name)
            
        df = df.withColumn('pause_dist', F.when(F.col(index).isNotNull() &
                                                (F.col('check')  == 1) &
                                                (F.lag('state', 1).over(w2) == 3) &
                                                (F.lag('tripType', 1).over(w2) == 2) &
                                                (F.col('state') == 3) &
                                                (F.col('tripType') == 3) &
                                                (F.col('pause') == F.col('duration')),
                                                0.0).otherwise(F.col('pause_dist'))
                          ).orderBy(ts_name)
        
        ###################+CHECK THIS BLOCK
        df = df.withColumn('state', F.when(F.col(index).isNotNull() &
                                           (F.lag('check',1).over(w2) == 1) &
                                           (F.lag('state', 2).over(w2) == 3) &
                                           (F.lag('tripType', 2).over(w2) == 2) &
                                           (F.lag('state',1).over(w2) == 3) &
                                           (F.lag('tripType',1).over(w2) == 3) &
                                           (F.lag('pause',1).over(w2) == F.lag('duration',1).over(w2)) &
                                           (F.lag('pause_dist',1).over(w2) == 0.0),
                                           0).otherwise(F.col('state'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              (F.lag('check',1).over(w2) == 1) &
                                              (F.lag('state', 2).over(w2) == 3) &
                                              (F.lag('tripType', 2).over(w2) == 2) &
                                              (F.lag('state',1).over(w2) == 3) &
                                              (F.lag('tripType',1).over(w2) == 3) &
                                              (F.lag('pause',1).over(w2) == F.lag('duration',1).over(w2)) &
                                              (F.lag('pause_dist',1).over(w2) == 0.0),
                                              0).otherwise(F.col('tripType'))
                          ).orderBy(ts_name)
        ###################+
        
    elif FLAG == 'CASE3':
            
        w2 = Window.partitionBy('segment').orderBy(ts_name)
        w3 = Window.partitionBy('segment').orderBy(ts_name).rowsBetween(Window.unboundedPreceding, 0)
            
        df = df.withColumn('state', F.when(F.col(index).isNotNull() &
                                           (F.col('pause') <= max_pause_time),
                                           0).otherwise(F.col('state'))
                          ).orderBy(ts_name)
            
        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              (F.col('pause') <= max_pause_time),
                                              0).otherwise(F.col('tripType'))
                          ).orderBy(ts_name)
            
        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              (F.col('pause') == F.col('duration')),
                                              4
                                             ).otherwise(F.col('tripType'))
                          ).orderBy(ts_name)
            
        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              (F.lag('tripType', 1).over(w2) == 0) &
                                              (F.lag('state', 1).over(w2) == 0) &
                                              (F.col('state') == 2) & ###########+CHECK
                                              (F.col('tripType') != 0),
                                              1
                                             ).otherwise(F.col('tripType'))
                          ).orderBy(ts_name)
        
        ###########+CHECK THIS BLOCK -- relevant when CASE3 after merging of segments in CASE4
        df = df.withColumn('state', F.when(F.col(index).isNotNull() &
                                           (F.col('pause') > max_pause_time) &
                                           (F.col('cum_dist_min') < min_dist_per_min),
                                           0).otherwise(F.col('state'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              (F.col('pause') > max_pause_time) &
                                              (F.col('cum_dist_min') < min_dist_per_min),
                                              0).otherwise(F.col('tripType'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('state', F.when(F.col(index).isNotNull() &
                                           (F.col('pause') > max_pause_time) &
                                           (F.col('cum_dist_min') < min_dist_per_min) &
                                           (F.col('state') == 0) &
                                           (F.col('tripType') == 0) &
                                           (F.lag('state',1).over(w2) == 2),
                                           3).otherwise(F.col('state'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              (F.col('pause') > max_pause_time) &
                                              (F.col('cum_dist_min') < min_dist_per_min) &
                                              (F.col('state') == 3) &
                                              (F.col('tripType') == 0) &
                                              (F.lag('state',1).over(w2) == 2),
                                              2).otherwise(F.col('tripType'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('state', F.when(F.col(index).isNotNull() &
                                           (F.col('pause') > max_pause_time) &
                                           (F.col('cum_dist_min') < min_dist_per_min) &
                                           (F.col('state') == 0) &
                                           (F.col('tripType') == 0) &
                                           (F.lag('state',1).over(w2) == 3) &
                                           (F.lag('tripType',1).over(w2) == 2) &
                                           (F.lag('state',2).over(w2) == 2),
                                           3).otherwise(F.col('state'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              (F.col('pause') > max_pause_time) &
                                              (F.col('cum_dist_min') < min_dist_per_min) &
                                              (F.col('state') == 3) &
                                              (F.col('tripType') == 0) &
                                              (F.lag('state',1).over(w2) == 3) &
                                              (F.lag('tripType',1).over(w2) == 2) &
                                              (F.lag('state',2).over(w2) == 2),
                                              3).otherwise(F.col('tripType'))
                          ).orderBy(ts_name)
        
        ###########+
            
        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              (F.lag('tripType', 1).over(w2) == 4) &
                                              (F.lag('state', 1).over(w2) == 0) &
                                              (F.col('tripType') != 0),
                                              1
                                             ).otherwise(F.col('tripType'))
                          ).orderBy(ts_name)
            
        df = df.withColumn('state', F.when(F.col(index).isNotNull() &
                                           (F.lag('state', 1).over(w2) == 0) &
                                           (F.lag('tripType', 1).over(w2) == 0) &
                                           (F.col('tripType') == 1),
                                           2).otherwise(F.col('state'))
                          ).orderBy(ts_name)
        
        
        df = df.withColumn('check', F.when(F.col(index).isNotNull() &
                                              (F.lag('state', 1).over(w2) == 3) &
                                              (F.lag('tripType', 1).over(w2) == 2) &
                                              F.lag('pause2', 3).over(w2).isNull() &
                                              F.lag('pause2', 2).over(w2).isNotNull(),
                                              1).otherwise(F.col('check'))
                          ).orderBy(ts_name)
            
        df = df.withColumn('pause', F.when(F.col(index).isNotNull() &
                                           (F.col('check')  == 1) &
                                           (F.lag('state', 1).over(w2) == 3) &
                                           (F.lag('tripType', 1).over(w2) == 2) &
                                           (F.col('state') == 3) &
                                           (F.col('tripType') == 3) &
                                           (F.col('pause') != F.col('duration')),
                                           F.col('duration')).otherwise(F.col('pause'))
                          ).orderBy(ts_name)
            
        df = df.withColumn('pause_dist', F.when(F.col(index).isNotNull() &
                                                (F.col('check')  == 1) &
                                                (F.lag('state', 1).over(w2) == 3) &
                                                (F.lag('tripType', 1).over(w2) == 2) &
                                                (F.col('state') == 3) &
                                                (F.col('tripType') == 3) &
                                                (F.col('pause') == F.col('duration')),
                                                0.0).otherwise(F.col('pause_dist'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('check', F.when(F.col(index).isNotNull() &
                                           (F.lag('state', 1).over(w2) == 3) &
                                           (F.lag('tripType', 1).over(w2) == 2) &
                                           (F.lead('state', 1).over(w2) == 0) &
                                           (F.lead('tripType', 1).over(w2) == 0) &
                                           (F.col('state') == 3) &
                                           (F.col('tripType') == 3) &
                                           (F.col('pause') != F.col('duration')),
                                           0)
                          )
        df = df.withColumn('check', F.when(F.col(index).isNotNull() & 
                                           F.col('check').isNull(),
                                           F.last('check', ignorenulls=True).over(w3)
                                          ).otherwise(F.col('check'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('pause', F.when(F.col(index).isNotNull() &
                                           (F.col('check')  == 0) &
                                           F.lag('check', 1).over(w2).isNull(),
                                           F.col('duration')).otherwise(F.col('pause'))
                          ).orderBy(ts_name)
            
        df = df.withColumn('pause_dist', F.when(F.col(index).isNotNull() &
                                                (F.col('check')  == 0) &
                                                F.lag('check', 1).over(w2).isNull(),
                                                0.0).otherwise(F.col('pause_dist'))
                          ).orderBy(ts_name)
        
        ############+CHECK THIS BLOCK
        df = df.withColumn('state', F.when(F.col(index).isNotNull() &
                                           (F.lag('check',1).over(w2) == 0) &
                                           F.lag('check', 2).over(w2).isNull() &
                                           (F.lag('pause',1).over(w2) == F.lag('duration',1).over(w2)),
                                           0).otherwise(F.col('state'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              (F.lag('check',1).over(w2) == 0) &
                                              F.lag('check', 2).over(w2).isNull() &
                                              (F.lag('pause',1).over(w2) == F.lag('duration',1).over(w2)),
                                              0).otherwise(F.col('tripType'))
                          ).orderBy(ts_name)
        ############+
        
        df = df.withColumn('state', F.when(F.col(index).isNotNull() &
                                           (F.lead('state', 1).over(w2) == 0) &
                                           (F.lead('tripType', 1).over(w2) == 0) &
                                           (F.col('tripType') == 1) &
                                           (F.col('state') == 2),
                                           0).otherwise(F.col('state'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              (F.lead('tripType', 1).over(w2) == 0) &
                                              (F.lead('state', 1).over(w2) == 0) &
                                              (F.col('tripType') == 1) &
                                              (F.col('state') == 0),
                                              0
                                             ).otherwise(F.col('tripType'))
                          ).orderBy(ts_name)
        
    elif FLAG == 'CASE4':
        
        w = Window.orderBy(ts_name)
        w2 = Window.partitionBy('segment').orderBy(ts_name).rowsBetween(0, Window.unboundedFollowing)
        w3 = Window.partitionBy('segment').orderBy(ts_name).rowsBetween(Window.unboundedPreceding, 0)
        w4 = Window.partitionBy('check').orderBy(ts_name).rowsBetween(Window.unboundedPreceding, 0)
        
        
        df = df.withColumn('check', F.when(F.col(index).isNotNull() &
                                           (F.last('pause').over(w2) <= max_pause_time) &
                                           (F.last('pause_dist').over(w2) < min_dist_per_min), 
                                           1).otherwise(None)
                          ).orderBy(ts_name)
        
        df = df.withColumn('check', F.when((F.lag('check',1).over(w) == 1) &
                                           F.col('check').isNull(),
                                           2
                                          ).otherwise(F.col('check'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('check', F.when(F.col('check').isNull(),
                                           F.last('check', ignorenulls=True).over(w3)
                                          ).otherwise(F.col('check'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('pause', F.when((F.col('check') == 2) &
                                           (F.lag('check', 1).over(w) == 1),
                                           F.lag('pause', 1).over(w)
                                          ).otherwise(F.col('pause'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('pause_dist', F.when((F.col('check') == 2) &
                                                (F.lag('check', 1).over(w) == 1),
                                                F.lag('pause_dist', 1).over(w)
                                               ).otherwise(F.col('pause_dist'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('segment', F.when((F.col('check') == 2),
                                             None).otherwise(F.col('segment'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('segment', F.when((F.col('check') == 2) &
                                             (F.col('pause') == F.lag('pause',1).over(w)) &
                                             (F.lag('check').over(w) == 1),
                                             F.lag('segment', 1).over(w)
                                            ).otherwise(F.col('segment'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('segment', F.when(F.col('segment').isNull() &
                                             (F.col('check') == 2),
                                             F.last('segment', ignorenulls=True).over(w4) 
                                            ).otherwise(F.col('segment'))
                          ).orderBy(ts_name)
        
        ##################+reset state and tripType
        df = df.withColumn('state', F.when(F.col('check') == 2, 
                                           F.col('state_cp')
                                          ).otherwise(F.col('state'))
                          )
        
        df = df.withColumn('tripType', F.when(F.col('check') == 2, 
                                              F.col('tripType_cp')
                                             ).otherwise(F.col('tripType'))
                          )  
        ##################+TO BE CHECKED
        
        df = df.withColumn(index, F.when((F.col('check') == 2),
                                         None).otherwise(F.col(index))
                          ).orderBy(ts_name)
        
        df = df.withColumn(index, F.when((F.col('check') == 2),
                                         F.last(index, ignorenulls=True).over(w3)
                                        ).otherwise(F.col(index))
                          ).orderBy(ts_name)
        
        df = df.withColumn('i1', F.when((F.col('check') == 2),
                                         None).otherwise(F.col('i1'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('i1', F.when((F.col('check') == 2),
                                         F.last('i1', ignorenulls=True).over(w3)
                                        ).otherwise(F.col('i1'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('i2', F.when((F.col('check') == 2),
                                         None).otherwise(F.col('i2'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('i2', F.when((F.col('check') == 2),
                                         F.last('i2', ignorenulls=True).over(w3)
                                        ).otherwise(F.col('i2'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('i3', F.when((F.col('check') == 2),
                                         None).otherwise(F.col('i3'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('i3', F.when((F.col('check') == 2),
                                         F.last('i3', ignorenulls=True).over(w3)
                                        ).otherwise(F.col('i3'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('j4', F.when((F.col('check') == 2),
                                         None).otherwise(F.col('j4'))
                          ).orderBy(ts_name)
        
        df = df.withColumn('j4', F.when((F.col('check') == 2),
                                         F.last('j4', ignorenulls=True).over(w3)
                                        ).otherwise(F.col('j4'))
                          ).orderBy(ts_name)
            
    df = df.drop(*['check','check2','pause2']) 
                        
    return df

##########################################################################################################
    
def set_pause(df, index, ts_name):
        
    """
        run after proc_segment
        start from  set: 3 3 duration 0.0
    """
    
    # Calculate distance between two fixes
    app_fun = F.udf(lambda a,b,c,d: calc_distance(a,b,c,d))
    
    w = Window.orderBy(ts_name)
    w2 = Window.partitionBy('segment').orderBy(ts_name)
    w3 = Window.partitionBy('segment').orderBy(ts_name).rowsBetween(Window.unboundedPreceding,
                                                                    Window.unboundedFollowing)
    pcol = 'pause2'
    
    df2 = df.withColumn(pcol, F.when(F.col(index).isNotNull() &
                                     (F.lag('state', 1).over(w2) == 3) &
                                     (F.lag('tripType', 1).over(w2) == 2) &
                                     (F.col('state') == 3) &
                                     (F.col('tripType') == 3) &
                                     (F.lead('tripType', 1).over(w2) != 3) &  ####### CHECK
                                     (F.col('pause') == F.col('duration')),
                                     F.col('duration')
                                     )
                       ).orderBy(ts_name)
    
    # avoid to pick up multiple lines with pcol non-null
    df2 = df2.withColumn('check', F.when(F.col(index).isNotNull(),
                                      F.last(pcol, ignorenulls=True)
                                       .over(w2.rowsBetween(Window.unboundedPreceding, 0))
                                     ).otherwise(F.col(pcol))
                        ).orderBy(ts_name)
    
    df2 = df2.withColumn(pcol, F.when(F.col(index).isNotNull() &
                                     F.col(pcol).isNotNull() &
                                     F.lag('check',1).over(w2).isNull(),
                                     F.col(pcol)
                                     )
                       ).drop('check').orderBy(ts_name)
    
    df2 = df2.withColumn('pause_cp', F.when(F.col(index).isNotNull(), F.col('pause')))           
    df2 = df2.withColumn('pause_dist_cp', F.when(F.col(index).isNotNull(),F.col('pause_dist')))  
    
    df2 = df2.withColumn(pcol, F.when(F.col(index).isNotNull(),
                                      F.last(pcol, ignorenulls=True)
                                       .over(w2.rowsBetween(0, Window.unboundedFollowing))
                                     ).otherwise(F.col(pcol))
                        ).orderBy(ts_name)
    
    
    # Calculate pause-time for merged segments
    df2 = df2.withColumn('pause', F.when(F.col(index).isNotNull() &
                                         F.col(pcol).isNotNull() &
                                         F.lead(pcol,1).over(w2).isNull() &
                                         (F.col('pause') == F.col('duration')),
                                         F.col('cum_pause') - F.col('duration')
                                        ).otherwise(F.col('pause'))
                        ).orderBy(ts_name)
        
    df2 = df2.withColumn('pause', F.when(F.col(index).isNotNull() &
                                         F.col(pcol).isNull() &
                                         (F.col('pause') != F.col('cum_pause') - F.col('duration')),
                                         None
                                        ).otherwise(F.col('pause'))
                        )
        
    df2 = df2.withColumn('pause', F.when(F.col(index).isNotNull() &
                                         F.col(pcol).isNull(), 
                                         F.last('pause', ignorenulls=True).over(w2)
                                        ).otherwise(F.col('pause'))
                        ).orderBy(ts_name)
    
    df2 = df2.withColumn(pcol, F.when(F.col(index).isNotNull() &
                                      (F.col('pause') == F.col('cum_pause') - F.col('duration')),
                                      None
                                     ).otherwise(F.col(pcol))
                        )
        
    df2 = df2.withColumn('pause', F.when(F.col(index).isNotNull() &
                                         F.col(pcol).isNull(),
                                         F.col('cum_pause') - F.col('pause')
                                        ).otherwise(F.col('pause'))
                        )
        
       
    # Compute the distance traveled from the beginning of a pause
    df2 = df2.withColumn('pause_dist', F.when(F.col(index).isNotNull() &
                                              F.col(pcol).isNull() &
                                              (F.col('pause') != F.col('duration')),
                                              None
                                             ).otherwise(F.col('pause_dist'))
                        )
         
    df2 = df2.withColumn('lat2', F.when(F.col('pause_dist').isNotNull(), F.col('lat')))
    df2 = df2.withColumn('lat2', F.when(F.col('pause_dist').isNull(),
                                        F.last('lat2', ignorenulls=True).over(w2)
                                       )
                                  .otherwise(F.col('lat'))
                        ).orderBy(ts_name)
    df2 = df2.withColumn('lat2', F.when(F.col('lat2').isNull(), F.col('lat')).otherwise(F.col('lat2')))
                        
    df2 = df2.withColumn('lon2', F.when(F.col('pause_dist').isNotNull(), F.col('lon')))
    df2 = df2.withColumn('lon2', F.when(F.col('pause_dist').isNull(), 
                                        F.last('lon2', ignorenulls=True).over(w2)
                                       ).otherwise(F.col('lon'))
                            ).orderBy(ts_name)
    df2 = df2.withColumn('lon2', F.when(F.col('lon2').isNull(), F.col('lon')).otherwise(F.col('lon2')))
        
    df2 = df2.withColumn('pause_dist', F.when(F.col(index).isNotNull() &
                                              F.col(pcol).isNull(),
                                              app_fun(F.col('lat'),F.col('lon'),F.col('lat2'),F.col('lon2'))
                                             ).otherwise(F.col('pause_dist'))
                        )
   
    df2 = df2.withColumn('pause_dist', F.when(F.col(index).isNotNull() &
                                              F.col('pause').isNull(),
                                             F.col('pause_dist_cp')).otherwise(F.col('pause_dist'))
                        ).orderBy(ts_name)
    
    df2 = df2.withColumn('pause', F.when(F.col(index).isNotNull() &
                                         F.col('pause').isNull(),
                                         F.col('pause_cp')).otherwise(F.col('pause'))
                        ).orderBy(ts_name)
    
    df2 = df2.drop(*['lat2','lon2','pause_cp','pause_dist_cp',pcol])
    
    return df2
            
            
##########################################################################################################
        
def check_case(df, index, ts_name, min_dist_per_min, min_pause_duration, max_pause_time):
        """
        
        """
        
        w2 = Window.partitionBy('segment').orderBy(ts_name)
        
        pcol = 'pause2'
        df2 = df.withColumn(pcol, F.when(F.col(index).isNotNull() &
                                         (F.lag('state', 1).over(w2) == 3) &
                                         (F.lag('tripType', 1).over(w2) == 2) &
                                         (F.lead('tripType', 1).over(w2) != 3) & ##############
                                         (F.col('state') == 3) &
                                         (F.col('tripType') == 3) &
                                         (F.col('pause') == F.col('duration')),
                                         F.col('duration')
                                         )
                           ).orderBy(ts_name)
        
        df2 = df2.withColumn(pcol, F.when(F.col(index).isNotNull(),
                                          F.last(pcol, ignorenulls=True)
                                           .over(w2.rowsBetween(Window.unboundedPreceding, 0))
                                         ).otherwise(F.col(pcol))
                            ).orderBy(ts_name)
        
        ##### CASE 1
        df2 = df2.withColumn('j1', F.when(F.col(index).isNotNull() & 
                                           F.col(pcol).isNotNull() &
                                           (F.col('pause_dist') >= min_dist_per_min) &
                                           (F.col('pause') < min_pause_duration),
                                           11)
                            )
    
        df2 = df2.withColumn('j1', F.when(F.col(index).isNotNull() & 
                                           F.col(pcol).isNotNull() &
                                           F.col('j1').isNull(),
                                           F.last('j1', ignorenulls=True)
                                             .over(w2.rowsBetween(Window.unboundedPreceding,
                                                                  Window.unboundedFollowing)
                                                  )
                                          ).otherwise(F.col('j1'))
                            ).orderBy(ts_name)
    
        ##### CASE 2
        df2 = df2.withColumn('j2', F.when(F.col(index).isNotNull() & 
                                           F.col(pcol).isNotNull() &
                                           F.col('j1').isNull() &
                                           (F.col('pause_dist') >= min_dist_per_min) &
                                           (F.col('pause') >= min_pause_duration) &
                                           (F.col('pause') <= max_pause_time),
                                           22)
                            )
    
        df2 = df2.withColumn('j2', F.when(F.col(index).isNotNull() & 
                                           F.col(pcol).isNotNull() &
                                           F.col('j1').isNull() &
                                           F.col('j2').isNull(),
                                           F.last('j2', ignorenulls=True)
                                            .over(w2.rowsBetween(Window.unboundedPreceding,
                                                                 Window.unboundedFollowing)
                                                 )
                                          ).otherwise(F.col('j2'))
                            ).orderBy(ts_name)
   
        ##### CASE 3
        df2 = df2.withColumn('j3', F.when(F.col(index).isNotNull() & 
                                           F.col(pcol).isNotNull() &
                                           F.col('j1').isNull() &
                                           F.col('j2').isNull() &
                                           F.col('segment').isNotNull() &
                                           (F.col('segment') != 0) &
                                           (F.col('pause') > max_pause_time), ###### 
                                           33)
                            )
    
        df2 = df2.withColumn('j3', F.when(F.col(index).isNotNull() & 
                                           F.col(pcol).isNotNull() &
                                           F.col('j1').isNull() &
                                           F.col('j2').isNull() &
                                           F.col('j3').isNull() &
                                           F.col('segment').isNotNull() &
                                           (F.col('segment') != 0),
                                           F.last('j3', ignorenulls=True)
                                            .over(w2.rowsBetween(Window.unboundedPreceding,
                                                                 Window.unboundedFollowing)
                                                 )
                                           ).otherwise(F.col('j3'))
                            ).orderBy(ts_name)
        
        ###### CASE 4
        df2 = df2.withColumn('j4', F.when(F.col(index).isNotNull() & 
                                           F.col(pcol).isNotNull() &
                                           F.col('j1').isNull() &
                                           F.col('j2').isNull() &
                                           F.col('j3').isNull() &
                                           F.col('segment').isNotNull() &
                                           (F.col('segment') != 0) &
                                           (F.col('pause') <= max_pause_time), 
                                           44)
                            )
        
        df2 = df2.withColumn('j4', F.when(F.col(index).isNotNull() & 
                                           F.col(pcol).isNotNull() &
                                           F.col('j1').isNull() &
                                           F.col('j2').isNull() &
                                           F.col('j3').isNull() &
                                           F.col('j4').isNull() &
                                           F.col('segment').isNotNull() &
                                           (F.col('segment') != 0),
                                           F.last('j4', ignorenulls=True)
                                            .over(w2.rowsBetween(Window.unboundedPreceding,
                                                                 Window.unboundedFollowing)
                                                 )
                                           ).otherwise(F.col('j4'))
                            ).orderBy(ts_name)
        
        
        df2 = df2.drop(*[pcol])
             
        return df2

##########################################################################################################

def detect_trips(df, ts_name, dist_name, speed_name, fix_type_name, min_dist_per_min, 
                 min_pause_duration, max_pause_time, vmax):
    """
            
        
            state:  
                        - 0 = STATIONARY
                        - 2 = MOVING
                        - 3 = PAUSED
            
            tripType:  
                        - 0 = STATIONARY
                        - 1 = START POINT
                        - 2 = MID POINT
                        - 3 = PAUSE POINT
                        - 4 = END POINT
    
    """
    
    # Calculate distance between two fixes
    app_fun = F.udf(lambda a,b,c,d: calc_distance(a,b,c,d))
    
    # Set maximum distance travelled in ome minute
    max_dist_per_min = vmax * 1000/60 # meters
    
    minp = df.select(F.min(ts_name).cast('long')).first()[0]
    
    df2 = df.select('ID', ts_name, 'dow', 'lat', 'lon', dist_name, speed_name, fix_type_name)
    
    df2 = df2.withColumn('total_sec', F.col(ts_name).cast('long'))
    
    # Define duration of current fix
    df2 = df2.withColumn('duration', F.col(ts_name).cast(IntegerType())-
                                     F.lag(F.col(ts_name).cast(IntegerType()),1,minp)
                                      .over(Window.orderBy(ts_name))
                        )
    
    df2 = df2.withColumn('lat2', F.last('lat').over(Window.orderBy('total_sec')
                                                          .rangeBetween(0,60)
                                                    )
                        )
    df2 = df2.withColumn('lon2', F.last('lon').over(Window.orderBy('total_sec')
                                                          .rangeBetween(0,60)
                                                    )
                        )
    # Define cumulative traveled over one minute in the future (PALMS definition)
    df2 = df2.withColumn('cum_dist_min', app_fun(F.col('lat'),F.col('lon'),F.col('lat2'),F.col('lon2')))
    df2 = df2.drop(*['lat2','lon2','total_sec'])
   
    # Initialize initial state 
    df2 = df2.withColumn('state', F.lit(0))
    
    # Initialize tripType 
    df2 = df2.withColumn('tripType', F.lit(0))
    
    # Initialize pause column
    df2 = df2.withColumn('pause', F.lit(None))
    
    # Define cumulative duration
    df2 = df2.withColumn('cum_pause', F.sum('duration').over(Window.orderBy(ts_name)
                                                                   .rowsBetween(Window.unboundedPreceding,0)
                                                            )      
                        )
    # Cumulative distance during a PAUSED state
    df2 = df2.withColumn('pause_dist', F.lit(None))
    
    w = Window.orderBy(ts_name)
    
    # Set of conditions for a given state:
    
    #TODO: check if it is ok to replace the condition on the duration with one on fixTypeCode=2
    cond0b = (F.col('duration') == 0) & (F.col('cum_dist_min') >= min_dist_per_min) &\
             (F.col('cum_dist_min') <= max_dist_per_min) & (F.col('duration') <= 60)
    
    cond0c = (F.col('duration') == 0) &\
             ((F.col('cum_dist_min') < min_dist_per_min) | (F.col('cum_dist_min') > max_dist_per_min) |
              (F.col('duration') > 60)) 
    
    cond1 = (F.col('cum_dist_min') >= min_dist_per_min) & (F.col('duration') <= min_pause_duration)
    
    cond2 = (F.lag('state',1).over(w) == 2) &\
            (F.col('tripType') != 1) & (F.col('tripType') != 4) & (F.col('cum_dist_min') < min_dist_per_min) &\
            (F.col('duration') <= min_pause_duration)
    
    cond2a = (F.col('pause').isNull()) & (F.lag('state',1).over(w) == 3) &\
             (F.col('tripType') != 1) & (F.col('tripType') != 4) & (F.lag('tripType', 1).over(w) == 2)
    
    cond3 = (F.lag('state',1).over(w) == 2) & (F.col('duration') > max_pause_time)
    
    cond4 = (F.lag('state',1).over(w) == 2) & (F.col('duration') > min_pause_duration) &\
            (F.col('duration') <= max_pause_time)
    
    cond5 = (F.col('tripType') != 1) & (F.col('tripType') !=2) & (F.col('tripType') !=4) &\
            (F.col('duration') > max_pause_time)
    

    # 1. Define MOVING states and MIDPOINT fixes
    df2 = df2.withColumn('state', F.when(cond1, 2)
                                   .otherwise(F.col('state'))
                        )
    df2 = df2.withColumn('tripType', F.when(cond1, 2)
                                      .otherwise(F.col('tripType'))
                        )
    
    # 2. Define last fix as ENDPOINT and state as STATIONARY
    df2 = df2.withColumn('state', F.when(F.col('fixTypeCode') == 3, 0).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(F.col('fixTypeCode') == 3, 4).otherwise(F.col('tripType')))
    
    # 3. Define state and trip type of first tracking point 
    df2 = df2.withColumn('state', F.when(cond0b, 2).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(cond0b, 1).otherwise(F.col('tripType')))
    df2 = df2.withColumn('state', F.when(cond0c, 0).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(cond0c, 0).otherwise(F.col('tripType')))
    
    # 4. Define ENDPOINT based on pause duration cutoff while MOVING
    df2 = df2.withColumn('state', F.when(cond3, 0).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(cond3, 4).otherwise(F.col('tripType')))
    
    # 5. Define PAUSED state based on minimum pause duration while MOVING
    df2 = df2.withColumn('state', F.when(cond4, 3).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(cond4, 3).otherwise(F.col('tripType')))
    df2 = df2.withColumn('pause_dist', F.when(cond4, 0.0).otherwise(F.col('pause_dist')))
    df2 = df2.withColumn('pause', F.when(cond4, F.col('duration')).otherwise(F.col('pause')))
    
    # 5. Define PAUSED state while MOVING based on traveled future distance
    df2 = df2.withColumn('state', F.when(cond2, 3).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(cond2, 2).otherwise(F.col('tripType')))
    df2 = df2.withColumn('pause_dist', F.when(cond2a, 0.0).otherwise(F.col('pause_dist')))
    df2 = df2.withColumn('pause', F.when(cond2a, F.col('duration')).otherwise(F.col('pause')))
    df2 = df2.withColumn('state', F.when(F.col('pause').isNotNull(), 3).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(F.col('pause').isNotNull(), 3).otherwise(F.col('tripType')))
    
    # 6. Let's define segments at the beginning of a PAUSED state
    df2 = df2.withColumn('segment', F.when(F.col('pause').isNotNull(), F.monotonically_increasing_id() + 1))
    df2 = df2.withColumn('segment', F.when(F.col('segment').isNull(), 
                                           F.last('segment', ignorenulls=True)
                                            .over(Window.orderBy(ts_name)
                                                        .rowsBetween(Window.unboundedPreceding, 0)
                                                 )
                                          ).otherwise(F.col('segment'))
                        )
    
    df2 = df2.withColumn('segment', F.when(F.col('segment').isNull(), 0).otherwise(F.col('segment')))
    
    # 7. Partition over a given segment
    w2 = Window.partitionBy('segment').orderBy(ts_name)
    
    # 8. Compute the time passed since the beginning of the pause
    df2 = df2.withColumn('pause', F.when(F.col('pause').isNotNull(), F.col('cum_pause')-F.col('duration'))
                                   .otherwise(F.col('pause'))
                        )
    
    df2 = df2.withColumn('pause', F.when((F.col('tripType') != 1) &
                                         (F.col('tripType') != 4),
                                         F.col('cum_pause') - F.last('pause', ignorenulls=True).over(w2)
                                        ).otherwise(F.col('pause'))
                        ).orderBy(ts_name)
    
    # 9. Merge segments with duration smaller than max_pause_time
    df2 = df2.withColumn('idx', F.when((F.col('state') == 3) &
                                        (F.col('tripType') == 2) &
                                        (F.col('pause') <= max_pause_time),
                                       1)
                        )
    
    df2 = df2.withColumn('idx', F.when(F.col('idx').isNull(),
                                       F.last('idx', ignorenulls=True)
                                        .over(w2.rowsBetween(Window.unboundedPreceding,
                                                             Window.unboundedFollowing)
                                             )
                                      ).otherwise(F.col('idx'))
                        ).orderBy(ts_name)
    
    w3 = Window.partitionBy('idx').orderBy(ts_name)
    
    df2 = df2.withColumn('idx2', F.when((F.col('idx') == 1) &
                                        (F.col('state') == 3) &
                                        (F.col('tripType') == 3) &
                                        F.lag('idx',1).over(w).isNull(),
                                        F.monotonically_increasing_id())
                        )
    
    df2 = df2.withColumn('idx2', F.when(F.col('idx2').isNull(),
                                        F.last('idx2', ignorenulls=True)
                                         .over(w3)
                                        ).otherwise(F.col('idx2'))
                        ).orderBy(ts_name)
    
    df2 = df2.withColumn('idx', F.when(F.col('idx').isNotNull(), F.col('idx2')).otherwise(F.col('idx')))
    
    df2 = df2.drop('idx2')
   
    df2 = df2.withColumn('idx', F.when(F.col('idx').isNull() &
                                       F.lag('idx',1).over(w).isNotNull() &
                                       (F.lag('segment',1).over(w) == F.col('segment') - 1),
                                       (F.last('idx', ignorenulls=True).over(w))).otherwise(F.col('idx'))
                        ).orderBy(ts_name)
    
    df2 = df2.withColumn('idx', F.when(F.col('idx').isNull(), 
                                       F.last('idx', ignorenulls=True).over(w2)
                                      ).otherwise(F.col('idx'))
                        ).orderBy(ts_name)
    
    w3 = Window.partitionBy('idx').orderBy(ts_name)
    
    df2 = df2.withColumn('segment', F.when(F.col('idx').isNotNull(), 
                                           F.min('segment').over(w3)
                                          ).otherwise(F.col('segment'))
                        ).orderBy(ts_name)
    
    
    
    df2 = df2.withColumn('pause', F.when(F.col('idx').isNotNull(), 
                                        None
                                        ).otherwise(F.col('pause'))
                        )
    
    df2 = df2.withColumn('pause_dist', F.when(F.col('idx').isNotNull(),
                                            None).otherwise(F.col('pause_dist'))
                        )
  
    # 10. Recalculate pause-time for merged segments
    df2 = df2.withColumn('pause', F.when(F.col('idx').isNotNull() &
                                         (F.lag('tripType',1).over(w) == 2) &
                                         (F.lag('segment',1).over(w) < F.col('segment')),
                                         F.col('cum_pause') - F.col('duration')
                                        ).otherwise(F.col('pause'))
                        ).orderBy(ts_name)
    
    
    df2 = df2.withColumn('pause', F.when(F.col('idx').isNotNull(), 
                                         F.last('pause', ignorenulls=True).over(w)
                                        ).otherwise(F.col('pause'))
                        )
    
    df2 = df2.withColumn('pause', F.when(F.col('idx').isNotNull(),
                                         F.col('cum_pause') - F.col('pause')
                                        ).otherwise(F.col('pause'))
                        )
    
    df2 = df2.withColumn('pause_dist', F.when(F.col('idx').isNotNull() &
                                              (F.col('pause') == F.col('duration')),
                                              0.0).otherwise(F.col('pause_dist'))
                        )
    
    # 11. Compute the distance travelled from the beginning of a pause
    df2 = df2.withColumn('lat2', F.when(F.col('pause_dist').isNotNull(), F.col('lat')))
    df2 = df2.withColumn('lat2', F.when(F.col('pause_dist').isNull() &
                                        (F.col('tripType') != 1) & 
                                        (F.col('tripType') != 4),
                                        F.last('lat2', ignorenulls=True).over(w2)
                                       )
                                  .otherwise(F.col('lat'))
                        )
    df2 = df2.withColumn('lat2', F.when(F.col('lat2').isNull(), F.col('lat')).otherwise(F.col('lat2')))
    
                        
    df2 = df2.withColumn('lon2', F.when(F.col('pause_dist').isNotNull(), F.col('lon')))
    df2 = df2.withColumn('lon2', F.when(F.col('pause_dist').isNull() & 
                                        (F.col('tripType') != 1) & 
                                        (F.col('tripType') != 4), 
                                        F.last('lon2', ignorenulls=True).over(w2)
                                       )
                                  .otherwise(F.col('lon'))
                        )
    df2 = df2.withColumn('lon2', F.when(F.col('lon2').isNull(), F.col('lon')).otherwise(F.col('lon2')))
    
    df2 = df2.withColumn('pause_dist', app_fun(F.col('lat'),F.col('lon'),F.col('lat2'),F.col('lon2')))
    df2 = df2.drop(*['lat2','lon2'])
    
    # 12. Repartition of the segments into 3 distinct cases
    df2 = df2.drop('idx')
    
    ##### CASE 1
    df2 = df2.withColumn('i1', F.when((F.col('pause_dist') >= min_dist_per_min) &
                                      (F.col('pause') < min_pause_duration),
                                      1)
                        )
    
    df2 = df2.withColumn('i1', F.when(F.col('i1').isNull(),
                                        F.last('i1', ignorenulls=True)
                                         .over(w2.rowsBetween(Window.unboundedPreceding,
                                                              Window.unboundedFollowing)
                                              )
                                       ).otherwise(F.col('i1'))
                        ).orderBy(ts_name)
    
    ##### CASE 2
    df2 = df2.withColumn('i2', F.when(F.col('i1').isNull() &
                                      (F.col('pause_dist') >= min_dist_per_min) &
                                      (F.col('pause') <= max_pause_time),
                                      2)
                        )
    
    df2 = df2.withColumn('i2', F.when(F.col('i1').isNull() &
                                        F.col('i2').isNull(),
                                        F.last('i2', ignorenulls=True)
                                         .over(w2.rowsBetween(Window.unboundedPreceding,
                                                              Window.unboundedFollowing)
                                              )
                                       ).otherwise(F.col('i2'))
                        ).orderBy(ts_name)
   
    ##### CASE 3
    df2 = df2.withColumn('i3', F.when(F.col('i1').isNull() &
                                      F.col('i2').isNull() &
                                      F.col('segment').isNotNull() &
                                      (F.col('segment') != 0),
                                      3)
                        )
    
    df2 = df2.withColumn('i3', F.when(F.col('i1').isNull() &
                                        F.col('i2').isNull() &
                                        F.col('i3').isNull() &
                                        F.col('segment').isNotNull() &
                                        (F.col('segment') != 0),
                                        F.last('i3', ignorenulls=True)
                                         .over(w2.rowsBetween(Window.unboundedPreceding,
                                                              Window.unboundedFollowing)
                                              )
                                       ).otherwise(F.col('i3'))
                        ).orderBy(ts_name)
    
   
    # 13. Analyze first segment   
    df2 = df2.withColumn('tripType', F.when((F.col('segment') == 0) &
                                            (F.lag('state', 1).over(w) == 0) &
                                            (F.lag('tripType', 1).over(w) == 0) &
                                            (F.col('state') == 2) &
                                            (F.col('tripType') == 2),
                                            1).otherwise(F.col('tripType'))
                        ).orderBy(ts_name)
    
    ## create a copy of current state and tripType to be used when segments are merged in CASE4
    df2 = df2.withColumn('state_cp', F.col('state'))
    df2 = df2.withColumn('tripType_cp', F.col('tripType'))
    
    # 14. Process CASE 1
    stop = F.col('i1').isNotNull() & (F.col('pause_dist') == 0.0) & (F.col('pause') == F.col('duration'))
    
    df2 = proc_segment(df2, 'i1', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 3, 2, "CASE1")
    
    df2 = df2.withColumn('state_cp', F.when(F.col('i1').isNotNull(),
                                            F.col('state')
                                           ).otherwise(F.col('state_cp'))
                        )
    
    df2 = df2.withColumn('tripType_cp', F.when(F.col('i1').isNotNull(),
                                               F.col('tripType')
                                              ).otherwise(F.col('tripType_cp'))
                        )

    df2 = df2.cache()
    df2 = df2.checkpoint()
    df2.count()

    ct = df2.select('i1','duration','pause','pause_dist').filter(stop).count()
    
    ct_ = 0
    
    #print('i1:')
    #k=1
    while (ct - ct_ != 0):
        
        ct_ = ct

        df2 = df2.cache()
        df2 = df2.checkpoint()
        df2.count()
    
        df2 = set_pause(df2, 'i1', ts_name)
    
        df2 = check_case(df2, 'i1', ts_name, min_dist_per_min, min_pause_duration, max_pause_time)
    
        df2 = proc_segment(df2, 'j1', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 3, 2, "CASE1")
        df2 = proc_segment(df2, 'j2', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 3, 3, "CASE2")
        df2 = proc_segment(df2, 'j3', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 0, 0, "CASE3")
        df2 = proc_segment(df2, 'j4', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 0, 0, "CASE4")
        df2 = df2.drop(*['j1','j2','j3','j4'])
        
        #df2.cache()
    
        ct = df2.select('i1','duration','pause','pause_dist').filter(stop).count() 
        
        #print((k, ct, ct_))
        #k=k+1
    
        #df3.persist()
    
    # 15. Process CASE 2
    stop = F.col('i2').isNotNull() & (F.col('pause_dist') == 0.0) & (F.col('pause') == F.col('duration'))
    
    df2 = proc_segment(df2, 'i2', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 3, 3, "CASE2")
    
    df2 = df2.withColumn('state_cp', F.when(F.col('i2').isNotNull(),
                                            F.col('state')
                                           ).otherwise(F.col('state_cp'))
                        )
    
    df2 = df2.withColumn('tripType_cp', F.when(F.col('i2').isNotNull(),
                                               F.col('tripType')
                                              ).otherwise(F.col('tripType_cp'))
                        )

    df2 = df2.cache()
    df2 = df2.checkpoint()
    
    ct = df2.select('i2','duration','pause','pause_dist').filter(stop).count()
    
    ct_ = 0
    
    #print('i2:')
    #k=1
    while (ct - ct_ != 0):
        
        ct_ = ct

        df2 = df2.cache()
        df2 = df2.checkpoint()
        df2.count()
    
        df2 = set_pause(df2, 'i2', ts_name)
    
        df2 = check_case(df2, 'i2', ts_name, min_dist_per_min, min_pause_duration, max_pause_time)
    
        df2 = proc_segment(df2, 'j1', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 3, 2, "CASE1")
        df2 = proc_segment(df2, 'j2', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 3, 3, "CASE2")
        df2 = proc_segment(df2, 'j3', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 0, 0, "CASE3")
        df2 = proc_segment(df2, 'j4', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 0, 0, "CASE4")
        df2 = df2.drop(*['j1','j2','j3','j4'])
        
        #df2.cache()
    
        ct = df2.select('i2','duration','pause','pause_dist').filter(stop).count() 
        
        #print((k, ct, ct_))
        #k=k+1
    
        #df3.persist()
    
    # 16. Process CASE 3
    stop = F.col('i3').isNotNull() & (F.col('pause_dist') == 0.0) & (F.col('pause') == F.col('duration'))
    
    df2 = proc_segment(df2, 'i3', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 0, 0, "CASE3")
    
    df2 = df2.withColumn('state_cp', F.when(F.col('i3').isNotNull(),
                                            F.col('state')
                                           ).otherwise(F.col('state_cp'))
                        )
    
    df2 = df2.withColumn('tripType_cp', F.when(F.col('i3').isNotNull(),
                                               F.col('tripType')
                                              ).otherwise(F.col('tripType_cp'))
                        )

    df2 = df2.cache()
    df2 = df2.checkpoint()
    df2.count()
    
    ct = df2.select('i3','duration','pause','pause_dist').filter(stop).count()
    
    ct_ = 0
    
    #print('i3:')
    #k=1
    while (ct - ct_ != 0):
        
        ct_ = ct

        df2 = df2.cache()
        df2 = df2.checkpoint()
        df2.count()
    
        df2 = set_pause(df2, 'i3', ts_name)
    
        df2 = check_case(df2, 'i3', ts_name, min_dist_per_min, min_pause_duration, max_pause_time)
    
        df2 = proc_segment(df2, 'j1', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 3, 2, "CASE1")
        df2 = proc_segment(df2, 'j2', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 3, 3, "CASE2")
        df2 = proc_segment(df2, 'j3', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 0, 0, "CASE3")
        df2 = proc_segment(df2, 'j4', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 0, 0, "CASE4")
        df2 = df2.drop(*['j1','j2','j3','j4'])
        
        #df2.cache()
    
        ct = df2.select('i3','duration','pause','pause_dist').filter(stop).count() 
        
        #print((k, ct, ct_))
        #k=k+1
    
        #df3.persist()

    # 17. Sanity checks
    ## Redefine last fix as ENDPOINT and state as STATIONARY
    df2 = df2.withColumn('state', F.when(F.col('fixTypeCode') == 3, 0).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(F.col('fixTypeCode') == 3, 4).otherwise(F.col('tripType')))
    
    ## Correct isolated points with tripType=1
    df2 = df2.withColumn('tripType', F.when((F.col('state') == 2) &
                                            (F.col('tripType') == 1) &
                                            (F.lag('state',1).over(w2) == 2) &
                                            (F.lag('tripType',1).over(w2) == 2) &
                                            (F.lead('state',1).over(w2) == 2) &
                                            (F.lead('tripType',1).over(w2) == 2),
                                            2).otherwise(F.col('tripType'))
                        ).orderBy(ts_name)
    
    ## Set trip start after long break
    df2 = df2.withColumn('tripType', F.when((F.col('state') == 2) &
                                            (F.col('tripType') == 2) &
                                            (F.lag('state',1).over(w2) == 0) &
                                            (F.lag('tripType',1).over(w2) == 0),
                                            1).otherwise(F.col('tripType'))
                        ).orderBy(ts_name)
    
    df2 = df2.withColumn('tripType', F.when((F.col('state') == 2) &
                                            (F.col('tripType') == 2) &
                                            (F.lag('tripType',1).over(w2) == 4),
                                            1).otherwise(F.col('tripType'))
                        ).orderBy(ts_name)
    
    df2 = df2.withColumn('tripType', F.when((F.col('state') == 3) &
                                            (F.col('tripType') == 2) &
                                            (F.lead('tripType',1).over(w2) == 0) &
                                            (F.lead('state',1).over(w2) == 0),
                                            4).otherwise(F.col('tripType'))
                        ).orderBy(ts_name)
    
    #'duration'
    df2 = df2.drop(*['state_cp','tripType_cp','total_sec','cum_dist_min',
                    'pause', 'pause_dist','i1','i2','i3','state','segment'])
    
    return df2

##########################################################################################################

def trip_mode_type(roundSpeed, vehicle_speed_cutoff, bicycle_speed_cutoff, walk_speed_cutoff):
    
    """
         vehicle_speed_cutoff:  speeds greater than this value (in Km/h) will be marked as vehicle.

         bicycle_speed_cutoff:  speeds greater than this value (in Km/h) will be marked as bicycle.

         walk_speed_cutoff:     speeds greater than this value (in Km/h) will be marked as pedestrian.
    
    """
    
    # round the speed to the nearest integer
    speed_ = roundSpeed
    
    try:
        if speed_ < 0:
            value = -1 # unknown trip mode
        #elif speed_ >=0 and speed_ < walk_speed_cutoff:
        #    value = 0  # stationary (not in a trip)
        #elif speed_ >= walk_speed_cutoff and speed_ < bicycle_speed_cutoff:
        elif speed_ >= 0 and speed_ < bicycle_speed_cutoff:
            value = 1  # pedestrian trip
        elif speed_ >= bicycle_speed_cutoff and speed_ < vehicle_speed_cutoff:
            value = 2  # bicycle trip 
        elif speed_ >= vehicle_speed_cutoff:
            value = 3  # vehicle trip
    except:
        value = None
    
        
        
    return value

##########################################################################################################

def trip_segmentation(df, ts_name, speed_segment_length):
    
    """
    
    """
    
    w1 = Window.orderBy(ts_name).rowsBetween(Window.unboundedPreceding, 0)
    w2 = Window.partitionBy('segment').orderBy(ts_name)
    
    app_fun = F.udf(lambda a,b,c,d: calc_distance(a,b,c,d))
    
    # trip segmentation
    
    df2 = df.withColumn('segment', F.when(F.col('trip') == 1, F.monotonically_increasing_id()))
    df2 = df2.withColumn('segment', F.when(F.col('segment').isNull() &
                                           F.col('trip').isNotNull(),
                                           F.last('segment', ignorenulls=True).over(w1)
                                           ).otherwise(F.col('segment'))
                        ).orderBy(ts_name)
    
    # compute duration and traveled distance for each segment
    
    df2 = df2.withColumn('pause', F.when(F.col('trip') == 1,
                                         F.col('duration')
                                        )
                        )
    
    df2 = df2.withColumn('pause_dist', F.when(F.col('trip') == 1,
                                              0.0
                                             )
                        )
    
    df2 = df2.withColumn('pause', F.when(F.col('pause') == F.col('duration'),
                                         F.col('cum_pause') - F.col('duration')
                                        ).otherwise(F.col('pause'))
                        ).orderBy(ts_name)
        
    df2 = df2.withColumn('pause', F.when(F.col('segment').isNotNull() &
                                         F.col('pause').isNull(), 
                                         F.last('pause', ignorenulls=True).over(w1)
                                        ).otherwise(F.col('pause'))
                        ).orderBy(ts_name)
        
    df2 = df2.withColumn('pause', F.when(F.col('segment').isNotNull(),
                                         F.col('cum_pause') - F.col('pause')
                                        ).otherwise(F.col('pause'))
                        )
    
         
    df2 = df2.withColumn('lat2', F.when(F.col('pause_dist').isNotNull(), F.col('lat')))
    df2 = df2.withColumn('lat2', F.when(F.col('pause_dist').isNull(),
                                        F.last('lat2', ignorenulls=True).over(w2)
                                       )
                                  .otherwise(F.col('lat'))
                        ).orderBy(ts_name)
    df2 = df2.withColumn('lat2', F.when(F.col('lat2').isNull(), F.col('lat')).otherwise(F.col('lat2')))
                        
    df2 = df2.withColumn('lon2', F.when(F.col('pause_dist').isNotNull(), F.col('lon')))
    df2 = df2.withColumn('lon2', F.when(F.col('pause_dist').isNull(), 
                                        F.last('lon2', ignorenulls=True).over(w2)
                                       ).otherwise(F.col('lon'))
                            ).orderBy(ts_name)
    df2 = df2.withColumn('lon2', F.when(F.col('lon2').isNull(), F.col('lon')).otherwise(F.col('lon2')))
        
    df2 = df2.withColumn('pause_dist', F.when(F.col('segment').isNotNull() &
                                              F.col('pause_dist').isNull(),
                                              app_fun(F.col('lat'),F.col('lon'),F.col('lat2'),F.col('lon2'))
                                             ).otherwise(F.col('pause_dist'))
                        )
    df2 = df2.drop(*['lat2','lon2'])
    
    # remove short sub-trips 
    
    df2 = df2.withColumn('trip', F.when((F.col('trip') == 4) &
                                        (F.col('pause_dist') <= speed_segment_length) &
                                        F.lead('trip',1).over(Window.orderBy(ts_name)).isNotNull(),
                                        F.col('tripType')
                                       ).otherwise(F.col('trip'))
                        )
    
    df2 = df2.withColumn('trip', F.when((F.col('trip') == 1) &
                                        (F.lag('pause_dist',1).over(Window.orderBy(ts_name)) <= speed_segment_length),
                                        F.col('tripType')
                                       ).otherwise(F.col('trip'))
                        )
    
    return df2

##########################################################################################################

def classify_trips(df, ts_name, dist_name, speed_name, vehicle_speed_cutoff, bicycle_speed_cutoff, 
                   walk_speed_cutoff, min_trip_length, min_trip_duration, speed_segment_length, speed_percentile):
    
    """
    
    
    """
    w = Window.orderBy(ts_name).rowsBetween(0, Window.unboundedFollowing)
    w1 = Window.orderBy(ts_name).rowsBetween(Window.unboundedPreceding, 0)
    w2 = Window.partitionBy('segment').orderBy(ts_name)
    w3 = Window.partitionBy('tripMOT').orderBy(ts_name)
    
    udf_round = F.udf(lambda x: floor(x+0.5)) # floor(x+0.5) == Math.round(x) in JavaScript
    
    app_fun = F.udf(lambda x: trip_mode_type(x, vehicle_speed_cutoff, bicycle_speed_cutoff, walk_speed_cutoff))
    
    df2 = df.withColumn('tripMOT', F.lit(0)) 
    
    df2 = df2.withColumn('trip', F.when(F.col('tripType') == 1,
                                         1)
                        )
    
    df2 = df2.withColumn('trip', F.when(F.col('tripType') == 4,
                                         4).otherwise(F.col('trip'))
                        )
    
    df2 = df2.withColumn('trip', F.when(F.col('trip').isNull() &
                                        (F.col('tripType') == 2),
                                        F.col('tripType')
                                       ).otherwise(F.col('trip'))
                        ).orderBy(ts_name)
    
    
    # set trip start according to speed
    df2 = df2.withColumn('roundSpeed', F.when(F.col('trip').isNotNull(), 
                                              udf_round(F.col(speed_name)).cast(IntegerType())))
                                              #udf_round(F.col('speed2')).cast(IntegerType())))
    
    df2 = df2.withColumn('trip', F.when((F.col('trip') == 2) &
                                         (F.lag('roundSpeed',1).over(Window.orderBy(ts_name)) == 0),
                                         4).otherwise(F.col('trip'))
                        ).orderBy(ts_name)
    
    df2 = df2.withColumn('trip', F.when((F.col('trip') == 4) &
                                        (F.lag('trip',1).over(Window.orderBy(ts_name)) == 4),
                                        F.col('tripType')
                                       ).otherwise(F.col('trip'))
                        )
    
    df2 = df2.withColumn('trip', F.when((F.col('tripType') == 3) &
                                         (F.lag('tripType',1).over(Window.orderBy(ts_name)) == 2),
                                        4).otherwise(F.col('trip'))
                        ).orderBy(ts_name)
    
    df2 = df2.withColumn('trip', F.when((F.col('trip') == 2) &
                                         (F.lag('trip',1).over(Window.orderBy(ts_name)) == 4),
                                         1).otherwise(F.col('trip'))
                        ).orderBy(ts_name)
    
    df2 = df2.withColumn('trip', F.when(F.col('trip').isNotNull() &
                                        (F.col('tripType') == 2) &
                                        (F.lag('tripType',1).over(Window.orderBy(ts_name)) == 3),
                                        1).otherwise(F.col('trip'))
                        ).orderBy(ts_name)
    
    df2 = df2.withColumn('trip', F.when(F.col('trip').isNotNull() &
                                        (F.col('tripType') == 4) &
                                        (F.lead('tripType',1).over(Window.orderBy(ts_name)) == 0),
                                        4).otherwise(F.col('trip'))
                        ).orderBy(ts_name)
    
    df2 = trip_segmentation(df2, ts_name, speed_segment_length).checkpoint()
    
    stop = (F.col('trip') == 1)
    
    ct = df2.filter(stop).count()
    s = ct
    s_ = -1
    
    #k = 1
    while (s - s_ != 0):

        s_ = s
        
        ct_ = ct
        
        # identify segments within a trip
        df2 = trip_segmentation(df2, ts_name, speed_segment_length).checkpoint()
        
        ct = df2.filter(stop).count()
        
        s = ct+ct_
        
        #print((k, s, s_, ct, ct_))
        #k = k + 1
        
    df2 = trip_segmentation(df2, ts_name, speed_segment_length).checkpoint()
    
    # set trip mode
    #n_percentile = F.expr('percentile_approx(roundSpeed, {})'.format(str(speed_percentile)))
    n_percentile = F.expr('percentile(roundSpeed, {})'.format(str(speed_percentile*0.01)))
    
    df2 = df2.withColumn('tmp', n_percentile.over(Window.partitionBy('segment'))).orderBy(ts_name)                     
    
    df2 = df2.withColumn('tripMOT', F.when(F.col('tmp').isNotNull(),
                                           app_fun(F.col('tmp'))
                                          ).otherwise(F.col('tripMOT'))
                        )
    
    df2 = df2.withColumn('tripMOT', F.when(F.col('tripType') == 3, None).otherwise(F.col('tripMOT')))
    
    df2 = df2.drop(*['segment','pause','pause_dist','tmp']).orderBy(ts_name)
    
    df3 = df2.select(ts_name,'lat','lon','duration','distance','cum_pause','tripType','trip','tripMOT')\
             .filter(F.col('tripType') != 0).orderBy(ts_name)
    df2 = df2.drop(*['trip','tripMOT'])
    df3 = df3.filter(F.col('tripMOT').isNotNull()).orderBy(ts_name)
    
    df3 = df3.withColumn('ch', F.when(F.col('tripType') == 1, F.monotonically_increasing_id())
                        ).orderBy(ts_name)
    df3 = df3.withColumn('ch', F.when(F.col('ch').isNull() &
                                       (F.col('tripType') != 0),
                                       F.last('ch', ignorenulls=True)
                                        .over(Window.orderBy(ts_name).rowsBetween(Window.unboundedPreceding, 0))
                                      ).otherwise(F.col('ch'))
                        ).orderBy(F.col('ch'))
    
    # merge adjacent segments with equal tripMOT
    w4 = Window.partitionBy('ch').orderBy(ts_name)
    df3 = df3.withColumn('trip', F.when((F.col('trip') == 4) &
                                            (F.lead('trip',1).over(w4) == 1) &
                                            (F.col('tripMOT') == F.lead('tripMOT',1).over(w4)),
                                            F.col('tripType')
                                           ).otherwise(F.col('trip'))
                            ).orderBy(ts_name)
    
    df3 = df3.withColumn('trip', F.when((F.col('trip') == 1) &
                                            (F.lag('trip',1).over(w4) == F.lag('tripType',1).over(w4)) &
                                            (F.col('tripMOT') == F.lag('tripMOT',1).over(w4)),
                                            F.col('tripType')
                                           ).otherwise(F.col('trip'))
                            ).orderBy(ts_name)
    
    df3 = df3.withColumn('trip', F.when((F.col('trip') == 1) &
                                            (F.lag('tripType',1).over(w4) == 3),
                                            F.col('tripType')
                                           ).otherwise(F.col('trip'))
                            ).orderBy(ts_name)
    
    df3 = df3.withColumn('trip', F.when(F.col('trip').isNull() &
                                            (F.col('tripType') == 3),
                                            F.col('tripType')
                                           ).otherwise(F.col('trip'))
                            ).orderBy(ts_name)
    
    # trip segmentation
    df3 = trip_segmentation(df3, ts_name, speed_segment_length)
    
    # remove short trips
    df3 = df3.withColumn('cum_dist', F.sum('distance').over(w4.rowsBetween(Window.unboundedPreceding,0))
                        ).orderBy(ts_name)
    
    df3 = df3.withColumn('tmp', F.when((F.col('tripType') == 4) &
                                       ((F.col('cum_dist') < min_trip_length) |
                                        (F.col('pause') < min_trip_duration)),
                                       0)
                        ).orderBy(ts_name)
    
    df3 = df3.withColumn('tmp', F.when(F.col('tmp').isNull(),
                                       F.last('tmp', ignorenulls=True)
                                        .over(w4.rowsBetween(0, Window.unboundedFollowing))
                                      ).otherwise(F.col('tmp'))
                        ).orderBy(ts_name)
    
    df3 = df3.withColumn('tmp', F.when(F.col('tmp').isNull() &
                                       (F.col('trip') == 4) &
                                       ((F.col('cum_dist') < min_trip_length) |
                                        (F.col('pause') < min_trip_duration)),
                                       1).otherwise(F.col('tmp'))
                        ).orderBy(ts_name)
                        

    df3 = df3.withColumn('tmp', F.when(F.col('tmp').isNull(),
                                       F.last('tmp', ignorenulls=True)
                                        .over(w2.rowsBetween(0, Window.unboundedFollowing))
                                      ).otherwise(F.col('tmp'))
                        ).orderBy(ts_name)
    
    ## reset short isolated trips
    df3 = df3.withColumn('trip', F.when(F.col('tmp') == 0, 0).otherwise(F.col('trip')))
    df3 = df3.withColumn('tripMOT', F.when(F.col('tmp') == 0, 0).otherwise(F.col('tripMOT')))
    
    ## merge short trip segments
    df3 = df3.withColumn('tripMOT', F.when(F.col('tmp') == 1, None).otherwise(F.col('tripMOT')))
    
    df3 = df3.withColumn('tripMOT', F.when((F.col('tmp') == 1) &
                                           (F.col('trip') == 1),
                                           F.lag('tripMOT',1).over(Window.orderBy(ts_name))
                                          ).otherwise(F.col('tripMOT'))
                        ).orderBy(ts_name)
    
    df3 = df3.withColumn('tripMOT', F.when((F.col('tmp') == 1) &
                                           F.col('tripMOT').isNull(),
                                           F.last('tripMOT', ignorenulls=True)
                                            .over(w2.rowsBetween(Window.unboundedPreceding,0))
                                          ).otherwise(F.col('tripMOT'))
                        ).orderBy(ts_name)
    
    df3 = df3.withColumn('trip', F.when((F.col('tmp') == 1) &
                                        (F.col('trip') == 1),
                                        F.col('tripType')
                                       ).otherwise(F.col('trip'))
                        )
    
    df3 = df3.withColumn('trip', F.when(F.col('tmp').isNull() & 
                                        (F.lead('tmp',1).over(Window.orderBy(ts_name)) == 1) &
                                        (F.col('trip') == 4),
                                        F.col('tripType')
                                       ).otherwise(F.col('trip'))
                        )
    
    ## merge adjacent segments
    df3 = df3.withColumn('trip', F.when((F.col('trip') == 4) &
                                            (F.lead('trip',1).over(w4) == 1) &
                                            (F.col('tripMOT') == F.lead('tripMOT',1).over(w4)),
                                            F.col('tripType')
                                           ).otherwise(F.col('trip'))
                            ).orderBy(ts_name)
    
    df3 = df3.withColumn('trip', F.when((F.col('trip') == 1) &
                                            (F.lag('trip',1).over(w4) == F.lag('tripType',1).over(w4)) &
                                            (F.col('tripMOT') == F.lag('tripMOT',1).over(w4)),
                                            F.col('tripType')
                                           ).otherwise(F.col('trip'))
                            ).orderBy(ts_name)
    
    df3 = df3.withColumn('trip', F.when((F.col('trip') == 1) &
                                            (F.lag('tripType',1).over(w4) == 3),
                                            F.col('tripType')
                                           ).otherwise(F.col('trip'))
                            ).orderBy(ts_name)
    
    df3 = df3.withColumn('trip', F.when(F.col('trip').isNull() &
                                            (F.col('tripType') == 3),
                                            F.col('tripType')
                                           ).otherwise(F.col('trip'))
                            ).orderBy(ts_name)
    
    # trip segmentation
    df3 = trip_segmentation(df3, ts_name, speed_segment_length)
    df3 = df3.drop('ch').orderBy(ts_name).cache()
    
    df2 = df2.join(df3, [ts_name,'lat','lon','duration','distance','cum_pause',
                         'tripType'], how='left').orderBy(ts_name)
    df2 = df2.withColumn('tripMOT', F.when(F.col('tripType') == 3, 0).otherwise(F.col('tripMOT')))
    df2 = df2.withColumn('tripMOT', F.when(F.col('tripMOT').isNull(), 0).otherwise(F.col('tripMOT')))
    df2 = df2.withColumn('trip', F.when(F.col('trip').isNull(), F.col('tripType')).otherwise(F.col('trip')))
    
    df3.persist()                     
                         
    df2 = df2.drop(*['tmp','cum_dist','roundSpeed','pause','pause_dist','segment'])
    
    # compute trip number
    df2 = df2.withColumn('tripNumber', F.when(F.col('trip') == 1, F.monotonically_increasing_id()))
    df2 = df2.withColumn('tripNumber', F.when(F.col('tripNumber').isNull() &
                                              F.col('trip').isNotNull(),
                                              F.last('tripNumber', ignorenulls=True).over(w1)
                                             ).otherwise(F.col('tripNumber'))
                        ).orderBy(ts_name)
    df2 = df2.withColumn('tripNumber', F.when(F.col('tripNumber').isNotNull(),
                                              F.col('tripNumber') + F.lit(1)
                                             ).otherwise(F.col('tripNumber'))
                        )
    df2 = df2.withColumn('tripNumber', F.when(F.col('tripType') == 0, 0).otherwise(F.col('tripNumber')))
    df2 = df2.withColumn('tripNumber', F.when(F.col('tripNumber').isNull(), 0).otherwise(F.col('tripNumber')))
    
    # reset tripType
    df2 = df2.withColumn('tripType', F.col('trip')).orderBy(ts_name)
    df2 = df2.withColumn('tripNumber', F.when((F.col('tripMOT') == 0) &
                                              (F.col('tripType') == 0),
                                              0).otherwise(F.col('tripNumber'))
                         ).orderBy(ts_name)

    df2 = df2.select('ID',ts_name,'dow','lat','lon','fixTypeCode','tripNumber','tripType','tripMOT')
   
    return df2


##########################################################################################################
