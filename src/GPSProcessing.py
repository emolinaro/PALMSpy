"""
MIT License

Copyright (c) 2019 Emiliano Molinaro

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
from math import *
import numpy as np

##########################################################################################################

def gen_gps_dataframe(df, ts_name, datetime_format):
    """
    Generate GPS DataFrame from raw data with timestamp column defined according to given datetime format.
    The other columns are:
        - day of the week (dow)
        - latitude (lat)
        - longitude (lon)
        - GPS distance (distance)
        - GPS elevation (height)
        - GPS velocity (speed)

    :param df:
        Spark DataFrame object containing the GPS raw data
    :param ts_name:
        name of timestamp column
    :param datetime_format:
        datetime format assigned to raw data
    :return:
        Spark DataFrame object with timestamp column defined according to given datetime format.
    """

    def utc_to_local(utc_dt, lat, lon):
        import pytz
        import timezonefinder

        tf = timezonefinder.TimezoneFinder()

        timezone_str = tf.timezone_at(lat=lat, lng=lon)

        if timezone_str is None:
            print("Could not determine the time zone")
        else:
            # Display the current time in that time zone
            timezone = pytz.timezone(timezone_str)
            try:
                utcoff = timezone.utcoffset(utc_dt)
            except pytz.exceptions.AmbiguousTimeError:
                print('pytz.exceptions.AmbiguousTimeError: %s' % utc_dt)
                utcoff = timezone.utcoffset(utc_dt, is_dst=True)

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

    ## convert column names to lowercase and remove spaces
    ## rename the colums for the speed and the height
    cols = df.columns
    cols = [c.lower() for c in cols]
    cols = [col.replace('(km/h)', '') for col in cols]
    cols = [col.replace('(m)', '') for col in cols]
    cols = [c.strip().replace(' ', '_') for c in cols]
    cols = [col.replace('n_s', 'n/s') for col in cols]
    cols = [col.replace('e_w', 'e/w') for col in cols]

    gps_data = df.toDF(*cols)
    gps_data.cache()

    ## drop duplicates
    gps_data = gps_data.drop_duplicates()  # TODO: implement better this condition: avoid to drop valid points

    header = gps_data.columns

    ## rename third and fourth columns, which define date and time, respectively
    gps_data = gps_data.withColumn("date", F.col(header[2]))
    gps_data = gps_data.drop(header[2])
    gps_data = gps_data.withColumn("time", F.col(header[3]))
    gps_data = gps_data.drop(header[3])

    ## define latitude and longitude with sign and remove duplicated columns
    gps_data = gps_data.withColumn('lat', F.when(remove_space_fun(F.col('n/s')) == 'S', F.abs(F.col('latitude')) * (-1)).otherwise(F.col('latitude'))).drop('latitude').drop('n/s')

    gps_data = gps_data.withColumn('lon', F.when(remove_space_fun(F.col('e/w')) == 'W', F.abs(F.col('longitude')) * (-1)).otherwise(F.col('longitude'))).drop('longitude').drop('e/w')

    ## define timestamps
    gps_data = gps_data.withColumn(ts_name,
                                   F.to_timestamp(
                                       F.concat(
                                           F.col('date'),
                                           F.lit(' '),
                                           F.col('time')
                                       ), datetime_format
                                   )
                                   )

    ## initialize the distance
    gps_data = gps_data.withColumn('distance', F.lit(0.0))

    ## convert speeds  in units of km/h
    gps_data = gps_data.withColumn('speed', convert_speed_fun(F.col('speed')).cast(DoubleType()))

    ## convert heights in units of meter
    gps_data = gps_data.withColumn('height', convert_height_fun(F.col('height')).cast(DoubleType()))

    ## calculate the offset with respect the UTC time
    sample = gps_data.sample(False, 0.1, seed=0).limit(10).select(ts_name, 'lat', 'lon')
    sample = sample.withColumn('ts', utc_to_local_fun(F.col(ts_name), F.col('lat'), F.col('lon')).cast(DoubleType()))
    sample = sample.select('ts').collect()
    sample = [int(row.ts / 3600.0) for row in sample]

    ## get the offset looking at the most frequent time interval in the list sample
    offset = max(set(sample), key=sample.count)

    ## apply time offset
    gps_data = gps_data.withColumn(ts_name,
                                   gps_data.timestamp + F.expr('INTERVAL {} HOURS'.format(str(offset)))
                                   )

    ## calculate day of the week
    gps_data = gps_data.withColumn('dow',
                                   F.date_format(ts_name, 'u')
                                   )

    gps_data = gps_data.select(ts_name, 'dow', 'lat', 'lon',
                               'distance', 'height', 'speed').orderBy(ts_name)

    ## drop NaNs
    gps_data = gps_data.dropna().orderBy(ts_name)

    return gps_data


##########################################################################################################

def round_timestamp(df, ts_name, interval):
    """
    Align timestamps according to given epoch interval.

    :param df:
        Spark DataFrame object with timestamp data
    :param ts_name:
        name of timestamp column
    :param interval:
        epoch duration (in seconds)
    :return:
        Spark DataFrame object with timestamp data
    """

    ## notice: use floor() instead of round() to match PALMS output

    data = df.withColumn("seconds", F.second(ts_name)) \
        .withColumn("round_seconds", F.floor(F.col("seconds") / interval) * interval) \
        .withColumn("add_seconds", (F.col("round_seconds") - F.col("seconds"))) \
        .withColumn("timestamp_sec", F.col(ts_name).cast("long")) \
        .withColumn("tot_seconds", F.col("timestamp_sec") + F.col("add_seconds")) \
        .drop("add_seconds").drop('timestamp_sec').drop('seconds').drop('round_seconds') \
        .withColumn(ts_name, F.col("tot_seconds").cast(dataType=TimestampType())).drop("tot_seconds")

    return data


##########################################################################################################

def calc_distance(lat1, lon1, lat2, lon2):
    """
    Implementation of the haversine formula.

    :param lat1:
        latitude of the first fix
    :param lon1:
        longitude of the first fix
    :param lat2:
        latitude of the second fix
    :param lon2:
        longitude of the first fix
    :return:
        distance between the two fixes
    """

    R = 6367000.0  # Earth average radius used by PALMS

    lat1R = radians(lat1)
    lon1R = radians(lon1)
    lat2R = radians(lat2)
    lon2R = radians(lon2)

    dlon = lon2R - lon1R
    dlat = lat2R - lat1R
    a = (sin(dlat / 2)) ** 2 + cos(lat1R) * cos(lat2R) * (sin(dlon / 2)) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c

    return distance


def haversine_dist(lat1, lon1, lat2, lon2):
    """
    Implementation of the haversine formula (not used in the code).

    :param lat1:
        latitude of the first fix
    :param lon1:
        longitude of the first fix
    :param lat2:
        latitude of the second fix
    :param lon2:
        longitude of the first fix
    :return:
        distance between the two fixes
    """

    ## Earth radius (km)
    R = 6367

    lat1, lon1, lat2, lon2 = map(np.deg2rad, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    dist = 2 * np.arcsin(np.sqrt(a)) * R

    return dist


##########################################################################################################

def set_fix_type(df, ts_name, fix_type_name, ws):
    """
    Set fix type code. Column fixTypeCode added to the DataFrame object, representing the fix type:

       -1 = unknown
        0 = invalid
        1 = valid (raw, unprocessed)
        2 = first fix
        3 = last fix
        4 = last valid fix
        5 = lone fix
        6 = inserted fix

    :param df:
        Spark DataFrame object with timestamp data
    :param ts_name:
        name of timestamp column
    :param fix_type_name:
        name of fix type column
    :param ws:
       signal loss window (in seconds)
    :return:
        Spark DataFrame object with timestamp data

    """

    minp = df.select(F.min(ts_name).cast('long')).first()[0]

    w = Window.orderBy(ts_name)

    df2 = df.withColumn('total_sec', F.col(ts_name).cast('long'))
    df2.cache()

    ## set fix type of first row as 'first fix'
    df2 = df2.withColumn(fix_type_name, F.when(F.col('total_sec') == minp, 2))

    ## set a new 'first fix' when the interval between current and previous timestamps
    ## is larger than the GPS loss signal duration, otherwise it is a 'valid' fix
    df2 = df2.withColumn(fix_type_name, F.when(F.col('total_sec') - F.lag('total_sec', 1, 0).over(w) > ws,
                                               2).otherwise(1)
                         )

    ## set 'lone fix' when the current fix and the following are both set to 'first fix'
    df2 = df2.withColumn(fix_type_name, F.when(F.col(fix_type_name) + F.lead(fix_type_name).over(w) == 4,
                                               5).otherwise(F.col(fix_type_name))
                         )

    ## filter out 'lone fixes'
    df2 = df2.filter(F.col(fix_type_name) != 5).orderBy(ts_name)

    ## set 'last fix'
    df2 = df2.withColumn(fix_type_name, F.when(F.lead(fix_type_name).over(w) == 2,
                                               3).otherwise(F.col(fix_type_name))
                         ).drop('total_sec')

    df2.select(df.columns + [fix_type_name])

    return df2


##########################################################################################################

def set_distance_and_speed(df, dcol, scol, tscol, fix_col):
    """
    Calculate for each fix the distance and speed at each epoch.

    :param df:
        Spark DataFrame object with timestamp data
    :param dcol:
        column with distance data
    :param scol:
        column with velocity data
    :param tscol:
        column with timestamp data
    :param fix_col:
        column with fix type code
    :return:
        Spark DataFrame object with timestamp data
    """

    ## calculate distance between two fixes
    app_fun = F.udf(lambda a, b, c, d: calc_distance(a, b, c, d))

    ## define a window over timestamps
    w = Window.orderBy(tscol)
    # df = df.drop_duplicates()

    minp = df.select(F.min(tscol).cast('long')).first()[0]

    df2 = df.withColumn('total_sec', F.col(tscol).cast('long'))

    ## define duration of current fix
    df2 = df2.withColumn('duration',
                         F.col(tscol).cast(IntegerType()) - F.lag(F.col(tscol).cast(IntegerType()), 1, minp).over(w)
                         )

    cond = (F.col(fix_col) == 1)

    first_lat = df2.select('lat').first()[0]
    first_lon = df2.select('lon').first()[0]
    last_lat = df2.select(F.last('lat')).first()[0]

    lat0 = F.col('lat').cast(DoubleType())
    lon0 = F.col('lon').cast(DoubleType())
    lat2 = F.lag(F.col('lat'), 1, first_lat).over(w)
    lon2 = F.lag(F.col('lon'), 1, first_lon).over(w)

    ## calculate the distance traveled from last fix
    df2 = df2.withColumn(dcol, F.when((F.col('lat') != first_lat) & (F.col('lat') != last_lat),
                                      app_fun(lat0, lon0, lat2, lon2)
                                      ).otherwise(0.0)
                         ).orderBy(tscol)

    ## calculate velocity
    df2 = df2.withColumn(scol, F.when(cond,
                                      3.6 * F.col(dcol) / F.col('duration')).otherwise(0.0)
                         )

    ## filter points where the velocity is not defined (points with the same timestamp)
    df2 = df2.filter(F.col(scol).isNotNull())
    df2 = df2.drop(*['duration', 'total_sec'])

    return df2


##########################################################################################################

def fill_timestamp(df, ts_name, fix_type_name, interval, ws):
    """
    Fill in missing timestamps with the last available data up to a given time interval.

    :param df:
        Spark DataFrame object with timestamp data
    :param ts_name:
        column with timestamp data
    :param fix_type_name:
        column with fix type code
    :param interval:
        epoch duration (in seconds)
    :param ws:
        signal loss window (in seconds)
    :return:
        Spark DataFrame object with timestamp data
    """

    minp, maxp = df.select(F.min(ts_name).cast('long'), F.max(ts_name).cast('long')).first()

    ## TODO: add SparkSession as input parameter
    spark = SparkSession.builder.getOrCreate()

    w1 = Window.orderBy('total_sec').rangeBetween(-ws - interval, 0)

    w2 = Window.orderBy('total_sec')

    ref = spark.range(minp, maxp, interval).select(F.col('id').cast(TimestampType()).alias(ts_name))
    ref = ref.cache()
    ref = ref.checkpoint()
    ref.count()

    ref = ref.join(df, [ts_name], how='leftouter')

    ref = ref.withColumn('total_sec', F.col(ts_name).cast('long'))

    columns = df.columns

    columns.remove(fix_type_name)

    for item in columns:
        ref = ref.withColumn(item, F.when(ref[item].isNotNull(),
                                          ref[item]).otherwise(F.last(ref[item], ignorenulls=True).over(w1))
                             )

    ## set 'last valid fix'
    ref = ref.withColumn(fix_type_name, F.when((F.col(fix_type_name).isNull()) &
                                               (F.last(fix_type_name, ignorenulls=True).over(w2) == 3),
                                               4).otherwise(F.col(fix_type_name))
                         )

    ## set 'inserted fix'
    ref = ref.withColumn(fix_type_name, F.when(F.col(fix_type_name).isNull(),
                                               6).otherwise(F.col(fix_type_name))
                         )

    ref = ref.drop('total_sec').dropna().orderBy(ts_name)

    try:

        ## sanity checks
        ref = ref.withColumn('tripNumber', F.when(F.col(fix_type_name) == 4, 0).otherwise(F.col('tripNumber')))
        ref = ref.withColumn('tripType', F.when(F.col(fix_type_name) == 4, 0).otherwise(F.col('tripType')))
        ref = ref.withColumn('tripMOT', F.when(F.col(fix_type_name) == 4, 0).otherwise(F.col('tripMOT')))

        ref = ref.withColumn('tripNumber', F.when((F.col(fix_type_name) == 6) &
                                                  (F.col('tripType') == 4),
                                                  0).otherwise(F.col('tripNumber'))
                             )
        ref = ref.withColumn('tripMOT', F.when((F.col(fix_type_name) == 6) &
                                               (F.col('tripType') == 4),
                                               0).otherwise(F.col('tripMOT'))
                             )
        ref = ref.withColumn('tripType', F.when((F.col(fix_type_name) == 6) &
                                                (F.col('tripType') == 4),
                                                0).otherwise(F.col('tripType'))
                             )
        ref = ref.withColumn('tripNumber', F.when((F.col(fix_type_name) == 6) &
                                                  (F.col('tripType') == 1),
                                                  0).otherwise(F.col('tripNumber'))
                             ).orderBy(ts_name)
        ref = ref.withColumn('tripMOT', F.when((F.col(fix_type_name) == 6) &
                                               (F.col('tripType') == 1),
                                               0).otherwise(F.col('tripMOT'))
                             ).orderBy(ts_name)
        ref = ref.withColumn('tripType', F.when((F.col(fix_type_name) == 6) &
                                                (F.col('tripType') == 1),
                                                0).otherwise(F.col('tripType'))
                             ).orderBy(ts_name)
        ref = ref.withColumn('tripNumber', F.when((F.col('tripType') == 1) &
                                                  (F.lead('tripType', 1).over(Window.orderBy(ts_name)) == 0),
                                                  0).otherwise(F.col('tripNumber'))
                             ).orderBy(ts_name)
        ref = ref.withColumn('tripMOT', F.when((F.col('tripType') == 1) &
                                               (F.lead('tripType', 1).over(Window.orderBy(ts_name)) == 0),
                                               0).otherwise(F.col('tripMOT'))
                             ).orderBy(ts_name)
        ref = ref.withColumn('tripType', F.when((F.col('tripType') == 1) &
                                                (F.lead('tripType', 1).over(Window.orderBy(ts_name)) == 0),
                                                0).otherwise(F.col('tripType'))
                             ).orderBy(ts_name)
        ref = ref.withColumn('tripType', F.when((F.col('tripType') == 2) &
                                                (F.lag('tripType', 1).over(Window.orderBy(ts_name)) == 0),
                                                1).otherwise(F.col('tripType'))
                             ).orderBy(ts_name)

    except:
        return ref

    return ref


##########################################################################################################

def filter_speed(df, vcol, fix_col, vmax):
    """
    Exclude data with velocity larger than a given value.

    :param df:
        Spark DataFrame object with timestamp data
    :param vcol:
        column with velocity data
    :param fix_col:
        column with fix type code
    :param vmax:
        velocity cutoff (in km/h)
    :return:
        Spark DataFrame object with timestamp data
    """

    cond = (F.col(fix_col) == 1)

    df = df.withColumn('check', F.when(cond & (F.col(vcol) > vmax), 1))

    df = df.filter(F.col('check').isNull())

    df = df.drop('check')

    return df


##########################################################################################################

def filter_height(df, hcol, tscol, fix_col, dhmax):
    """
    Exclude data where the change of height between two consecutive fixes is larger than a given value.

    :param df:
        Spark DataFrame object with timestamp data
    :param hcol:
        column with elevation data
    :param tscol:
        column with timestamp data
    :param fix_col:
        column with fix type code
    :param dhmax:
        height change cutoff (in meters)
    :return:
        Spark DataFrame object with timestamp data
    """

    cond = (F.col(fix_col) == 1)

    ## TODO: add SparkSession as input parameter
    spark = SparkSession.builder.getOrCreate()

    df.createOrReplaceTempView('df')
    ref = spark.sql("""
                    select *, {} - lag({}, 1, 0)
                    OVER (ORDER by {}) AS d_height
                    FROM df
                    """.format(hcol, hcol, tscol))

    ## if the first value of the height is zero, then the second row may be removed
    ref = ref.withColumn('check', F.when(cond & (F.abs(ref['d_height']) > dhmax), 1))

    ref = ref.filter(F.col('check').isNull())

    ref = ref.drop(*['check', 'd_height'])

    ## if the first value of the height is zero, this line inserts the second row back into the DataFrame
    ref = ref.union(df.limit(2)).orderBy(tscol).drop_duplicates()

    return ref


##########################################################################################################

def filter_change_dist_3_fixes(df, dcol, tscol, fix_col, dmin):
    """
    Exclude data where the minimum change in distance between three consecutive fixes is less than a given value.

    :param df:
        Spark DataFrame object with timestamp data
    :param dcol:
        column with distance data
    :param tscol:
        column with timestamp data
    :param fix_col:
        column with fix type code
    :param dmin:
        minimum distance over three consecutive fixes
    :return:
        Spark DataFrame object with timestamp data
    """

    ## calculate distance between two fixes
    app_fun = F.udf(lambda a, b, c, d: calc_distance(a, b, c, d))

    ## define a window over timestamps
    w = Window.orderBy(tscol)

    cond = (F.col(fix_col) == 1)

    first_lat = df.select('lat').first()[0]
    first_lon = df.select('lon').first()[0]
    last_lat = df.select(F.last('lat')).first()[0]
    last_lon = df.select(F.last('lon')).first()[0]

    lat0 = F.col('lat').cast(DoubleType())
    lon0 = F.col('lon').cast(DoubleType())
    lat1 = F.lead(F.col('lat'), 1, last_lat).over(w)
    lon1 = F.lead(F.col('lon'), 1, last_lon).over(w)
    lat2 = F.lag(F.col('lat'), 1, first_lat).over(w)
    lon2 = F.lag(F.col('lon'), 1, first_lon).over(w)

    ## recalculate the distance traveled from last fix
    df2 = df.withColumn(dcol, F.when((F.col('lat') != first_lat) & (F.col('lat') != last_lat),
                                     app_fun(lat0, lon0, lat2, lon2)
                                     ).otherwise(F.col(dcol))
                        )

    ## calculate distance between next fix and previous fix
    df2 = df2.withColumn('dist', F.when((F.col('lat') != first_lat) & (F.col('lat') != last_lat),
                                        app_fun(lat1, lon1, lat2, lon2)
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

def filter_acceleration(df, scol, tscol, fix_col, amax=7):
    """
    Exclude data points with acceleration larger than a given value.

    :param df:
        Spark DataFrame object with timestamp data
    :param scol:
        column with speed data
    :param tscol:
        column with timestamp data
    :param fix_col:
        column with fix type code
    :param amax:
        maximum acceleration
    :return:
        Spark DataFrame object with timestamp data
    """

    ## define a window over timestamps
    w = Window.orderBy(tscol)

    cond = (F.col(fix_col) == 1)

    ## calculate acceleration
    df2 = df.withColumn('acc', F.abs((F.col(scol) - F.lag(F.col(scol), 1).over(w))) /
                        (F.col(tscol).cast(IntegerType()) -
                         F.lag(F.col(tscol), 1).over(w).cast(IntegerType())) * 1000 / 3600
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
    Filter DataFrame with a new epoch duration.

    :param df:
        Spark DataFrame object with timestamp data
    :param ts_name:
        column with timestamp data
    :param window:
        new epoch duration (in seconds)
    :return:
        Spark DataFrame object with timestamp data
    """

    minp = df.select(F.min(ts_name).cast('long')).first()[0]

    df2 = df.withColumn('tmp', F.row_number().over(Window.orderBy(ts_name)) - 1)

    df2 = df2.withColumn('total_sec', F.col(ts_name).cast('long'))

    df2 = df2.withColumn('duration', F.col(ts_name).cast(IntegerType()) -
                         F.lag(F.col(ts_name).cast(IntegerType()), 1, minp).over(Window.orderBy(ts_name))
                         ).drop('total_sec')

    df2 = df2.withColumn('tmp', (F.col('tmp') * F.col('duration')) % window).drop('duration').orderBy(ts_name)

    df2 = df2.filter(F.col('tmp') == 0).drop('tmp').orderBy(ts_name)

    return df2


##########################################################################################################

def proc_segment(df, index, ts_name, min_dist_per_min, min_pause_duration, max_pause_time, state, trip, FLAG):
    """

    :param df:
        Spark DataFrame object with timestamp data
    :param index:
        fix index type: 'i1', 'i2', 'i3', 'j1', 'j2', 'j3'
    :param ts_name:
        column with timestamp data
    :param min_dist_per_min:
        minimum distance (in meters) travelled in one minute to define a trip
    :param min_pause_duration:
        minimum duration of a PAUSE fix type
    :param max_pause_time:
        minimum duration of END fix type
    :param state:
        fix state
    :param trip:
        fix trip type
    :param FLAG:
        string identifying segment type: 'CASE1', 'CASE2', 'CASE3', and 'CASE4
    :return:
        Spark DataFrame object with timestamp data
    """

    """
        set: 3 3 duration 0.0
    """

    if (index == 'i1') | (index == 'j1'):

        cond = F.col(index).isNotNull() & (F.col('pause_dist') >= min_dist_per_min) & \
               (F.col('pause') < min_pause_duration)

    elif (index == 'i2') | (index == 'j2'):

        cond = F.col(index).isNotNull() & (F.col('pause_dist') >= min_dist_per_min) & \
               (F.col('pause') >= min_pause_duration) & (F.col('pause') <= max_pause_time)

    elif (index == 'i3') | (index == 'j3'):

        cond = F.col(index).isNotNull() & (F.col('segment') != 0) & (F.col('pause') > max_pause_time)

    elif (index == 'j4'):

        ## sanity check: this is the case when the end of the segment has pause <= max_pause_time and
        ##               pause_dist < min_dist_per_min
        cond = F.col(index).isNotNull() & (F.col('pause_dist') < min_dist_per_min) & (F.col('pause') <= max_pause_time)

    else:

        cond = 0
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
                                           F.lag('pause2', 1).over(w2).isNull(),
                                           2
                                           ).otherwise(F.col('state'))
                           ).orderBy(ts_name)

        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              F.col('pause2').isNotNull() &
                                              F.lag('pause2', 1).over(w2).isNull(),
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
                                            F.last('check2', ignorenulls=True).over(w2)
                                            ).otherwise(F.col('check2'))
                           ).orderBy(ts_name)

        df = df.withColumn('pause', F.when(F.col(index).isNotNull() &
                                           (F.col('check2') == 1) &
                                           (F.lag('check2', 1).over(w2) == 0) &
                                           (F.lag('state', 1).over(w2) == 3) &
                                           (F.lag('tripType', 1).over(w2) == 2) &
                                           (F.col('state') == 3) &
                                           (F.col('tripType') == 3) &
                                           (F.col('pause') != F.col('duration')),
                                           F.col('duration')).otherwise(F.col('pause'))
                           ).orderBy(ts_name)

        df = df.withColumn('pause_dist', F.when(F.col(index).isNotNull() &
                                                (F.col('check2') == 1) &
                                                (F.lag('check2', 1).over(w2) == 0) &
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
                                           (F.col('check') == 1) &
                                           (F.lag('state', 1).over(w2) == 3) &
                                           (F.lag('tripType', 1).over(w2) == 2) &
                                           (F.col('state') == 3) &
                                           (F.col('tripType') == 3) &
                                           (F.col('pause') != F.col('duration')),
                                           F.col('duration')).otherwise(F.col('pause'))
                           ).orderBy(ts_name)

        df = df.withColumn('pause_dist', F.when(F.col(index).isNotNull() &
                                                (F.col('check') == 1) &
                                                (F.lag('state', 1).over(w2) == 3) &
                                                (F.lag('tripType', 1).over(w2) == 2) &
                                                (F.col('state') == 3) &
                                                (F.col('tripType') == 3) &
                                                (F.col('pause') == F.col('duration')),
                                                0.0).otherwise(F.col('pause_dist'))
                           ).orderBy(ts_name)

        ###################+CHECK THIS BLOCK
        df = df.withColumn('state', F.when(F.col(index).isNotNull() &
                                           (F.lag('check', 1).over(w2) == 1) &
                                           (F.lag('state', 2).over(w2) == 3) &
                                           (F.lag('tripType', 2).over(w2) == 2) &
                                           (F.lag('state', 1).over(w2) == 3) &
                                           (F.lag('tripType', 1).over(w2) == 3) &
                                           (F.lag('pause', 1).over(w2) == F.lag('duration', 1).over(w2)) &
                                           (F.lag('pause_dist', 1).over(w2) == 0.0),
                                           0).otherwise(F.col('state'))
                           ).orderBy(ts_name)

        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              (F.lag('check', 1).over(w2) == 1) &
                                              (F.lag('state', 2).over(w2) == 3) &
                                              (F.lag('tripType', 2).over(w2) == 2) &
                                              (F.lag('state', 1).over(w2) == 3) &
                                              (F.lag('tripType', 1).over(w2) == 3) &
                                              (F.lag('pause', 1).over(w2) == F.lag('duration', 1).over(w2)) &
                                              (F.lag('pause_dist', 1).over(w2) == 0.0),
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
                                              (F.col('state') == 2) &  ###########+CHECK
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
                                           (F.lag('state', 1).over(w2) == 2),
                                           3).otherwise(F.col('state'))
                           ).orderBy(ts_name)

        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              (F.col('pause') > max_pause_time) &
                                              (F.col('cum_dist_min') < min_dist_per_min) &
                                              (F.col('state') == 3) &
                                              (F.col('tripType') == 0) &
                                              (F.lag('state', 1).over(w2) == 2),
                                              2).otherwise(F.col('tripType'))
                           ).orderBy(ts_name)

        df = df.withColumn('state', F.when(F.col(index).isNotNull() &
                                           (F.col('pause') > max_pause_time) &
                                           (F.col('cum_dist_min') < min_dist_per_min) &
                                           (F.col('state') == 0) &
                                           (F.col('tripType') == 0) &
                                           (F.lag('state', 1).over(w2) == 3) &
                                           (F.lag('tripType', 1).over(w2) == 2) &
                                           (F.lag('state', 2).over(w2) == 2),
                                           3).otherwise(F.col('state'))
                           ).orderBy(ts_name)

        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              (F.col('pause') > max_pause_time) &
                                              (F.col('cum_dist_min') < min_dist_per_min) &
                                              (F.col('state') == 3) &
                                              (F.col('tripType') == 0) &
                                              (F.lag('state', 1).over(w2) == 3) &
                                              (F.lag('tripType', 1).over(w2) == 2) &
                                              (F.lag('state', 2).over(w2) == 2),
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
                                           (F.col('check') == 1) &
                                           (F.lag('state', 1).over(w2) == 3) &
                                           (F.lag('tripType', 1).over(w2) == 2) &
                                           (F.col('state') == 3) &
                                           (F.col('tripType') == 3) &
                                           (F.col('pause') != F.col('duration')),
                                           F.col('duration')).otherwise(F.col('pause'))
                           ).orderBy(ts_name)

        df = df.withColumn('pause_dist', F.when(F.col(index).isNotNull() &
                                                (F.col('check') == 1) &
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
                                           (F.col('check') == 0) &
                                           F.lag('check', 1).over(w2).isNull(),
                                           F.col('duration')).otherwise(F.col('pause'))
                           ).orderBy(ts_name)

        df = df.withColumn('pause_dist', F.when(F.col(index).isNotNull() &
                                                (F.col('check') == 0) &
                                                F.lag('check', 1).over(w2).isNull(),
                                                0.0).otherwise(F.col('pause_dist'))
                           ).orderBy(ts_name)

        ############+CHECK THIS BLOCK
        df = df.withColumn('state', F.when(F.col(index).isNotNull() &
                                           (F.lag('check', 1).over(w2) == 0) &
                                           F.lag('check', 2).over(w2).isNull() &
                                           (F.lag('pause', 1).over(w2) == F.lag('duration', 1).over(w2)),
                                           0).otherwise(F.col('state'))
                           ).orderBy(ts_name)

        df = df.withColumn('tripType', F.when(F.col(index).isNotNull() &
                                              (F.lag('check', 1).over(w2) == 0) &
                                              F.lag('check', 2).over(w2).isNull() &
                                              (F.lag('pause', 1).over(w2) == F.lag('duration', 1).over(w2)),
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

        df = df.withColumn('check', F.when((F.lag('check', 1).over(w) == 1) &
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
                                             (F.col('pause') == F.lag('pause', 1).over(w)) &
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

    df = df.drop(*['check', 'check2', 'pause2'])

    return df


##########################################################################################################

def set_pause(df, index, ts_name):
    """

    :param df:
        Spark DataFrame object with timestamp data
    :param index:
         fix index type: 'i1', 'i2', 'i3', 'j1', 'j2', 'j3'
    :param ts_name:
        column with timestamp data
    :return:
        Spark DataFrame object with timestamp data
    """

    """
        run after proc_segment
        start from  set: 3 3 duration 0.0
    """

    ## calculate distance between two fixes
    app_fun = F.udf(lambda a, b, c, d: calc_distance(a, b, c, d))

    w2 = Window.partitionBy('segment').orderBy(ts_name)
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

    ## avoid to pick up multiple lines with pcol non-null
    df2 = df2.withColumn('check', F.when(F.col(index).isNotNull(),
                                         F.last(pcol, ignorenulls=True)
                                         .over(w2.rowsBetween(Window.unboundedPreceding, 0))
                                         ).otherwise(F.col(pcol))
                         ).orderBy(ts_name)

    df2 = df2.withColumn(pcol, F.when(F.col(index).isNotNull() &
                                      F.col(pcol).isNotNull() &
                                      F.lag('check', 1).over(w2).isNull(),
                                      F.col(pcol)
                                      )
                         ).drop('check').orderBy(ts_name)

    df2 = df2.withColumn('pause_cp', F.when(F.col(index).isNotNull(), F.col('pause')))

    df2 = df2.withColumn('pause_dist_cp', F.when(F.col(index).isNotNull(), F.col('pause_dist')))

    df2 = df2.withColumn(pcol, F.when(F.col(index).isNotNull(),
                                      F.last(pcol, ignorenulls=True)
                                      .over(w2.rowsBetween(0, Window.unboundedFollowing))
                                      ).otherwise(F.col(pcol))
                         ).orderBy(ts_name)

    ## calculate pause-time for merged segments
    df2 = df2.withColumn('pause', F.when(F.col(index).isNotNull() &
                                         F.col(pcol).isNotNull() &
                                         F.lead(pcol, 1).over(w2).isNull() &
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

    ## compute the distance traveled from the beginning of a pause
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
                                              app_fun(F.col('lat'), F.col('lon'), F.col('lat2'), F.col('lon2'))
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

    df2 = df2.drop(*['lat2', 'lon2', 'pause_cp', 'pause_dist_cp', pcol])

    return df2


##########################################################################################################

def check_case(df, index, ts_name, min_dist_per_min, min_pause_duration, max_pause_time):
    """

    :param df:
        Spark DataFrame object with timestamp data
    :param index:
         fix index type: 'i1', 'i2', 'i3', 'j1', 'j2', 'j3'
    :param ts_name:
        column with timestamp data
    :param min_dist_per_min:
         minimum distance (in meters) travelled in one minute to define a trip
    :param min_pause_duration:
        minimum duration of a PAUSE fix type
    :param max_pause_time:
        minimum duration of END fix type
    :return:
        Spark DataFrame object with timestamp data
    """

    w2 = Window.partitionBy('segment').orderBy(ts_name)

    pcol = 'pause2'

    df2 = df.withColumn(pcol, F.when(F.col(index).isNotNull() &
                                     (F.lag('state', 1).over(w2) == 3) &
                                     (F.lag('tripType', 1).over(w2) == 2) &
                                     (F.lead('tripType', 1).over(w2) != 3) &  ##############
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
                                      (F.col('pause') > max_pause_time),  ######
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

def detect_trips(df, ts_name, dist_name, speed_name, fix_type_name,
                 min_dist_per_min, min_pause_duration, max_pause_time, vmax):
    """
    Trip detection: classify fix state and trip type.

            * state:
                        - 0 = STATIONARY
                        - 2 = MOVING
                        - 3 = PAUSED

            * tripType:
                        - 0 = STATIONARY
                        - 1 = START POINT
                        - 2 = MID POINT
                        - 3 = PAUSE POINT
                        - 4 = END POINT

    :param df:
        Spark DataFrame object with timestamp data
    :param ts_name:
        column with timestamp data
    :param dist_name:
        column with distance data
    :param speed_name:
        column with velocity data
    :param fix_type_name:
        column with fix type code
    :param min_dist_per_min:
         minimum distance (in meters) travelled in one minute to define a trip
    :param min_pause_duration:
        minimum duration of a PAUSE fix type
    :param max_pause_time:
        minimum duration of END fix type
    :param vmax:
        velocity cutoff (in km/h)
    :return:
        Spark DataFrame object with timestamp data
    """

    ## calculate distance between two fixes
    app_fun = F.udf(lambda a, b, c, d: calc_distance(a, b, c, d))

    ## set maximum distance travelled in ome minute
    max_dist_per_min = vmax * 1000 / 60  # meters

    minp = df.select(F.min(ts_name).cast('long')).first()[0]

    df1 = df.select(ts_name, 'dow', 'lat', 'lon', dist_name, speed_name, fix_type_name).cache()
    df1 = df1.checkpoint()
    df1.count()

    df2 = df.select(ts_name, 'lat', 'lon', fix_type_name)

    df2 = df2.withColumn('total_sec', F.col(ts_name).cast('long'))

    ## define duration of current fix
    df2 = df2.withColumn('duration', F.col(ts_name).cast(IntegerType()) -
                         F.lag(F.col(ts_name).cast(IntegerType()), 1, minp)
                         .over(Window.orderBy(ts_name))
                         )

    df2 = df2.withColumn('lat2', F.last('lat').over(Window.orderBy('total_sec')
                                                    .rangeBetween(0, 60)
                                                    )
                         )
    df2 = df2.withColumn('lon2', F.last('lon').over(Window.orderBy('total_sec')
                                                    .rangeBetween(0, 60)
                                                    )
                         )

    ## define cumulative traveled over one minute in the future (PALMS definition)
    df2 = df2.withColumn('cum_dist_min', app_fun(F.col('lat'), F.col('lon'), F.col('lat2'), F.col('lon2')))
    df2 = df2.drop(*['lat2', 'lon2', 'total_sec'])

    ## initialize initial state
    df2 = df2.withColumn('state', F.lit(0))

    ## initialize tripType
    df2 = df2.withColumn('tripType', F.lit(0))

    ## initialize pause column
    df2 = df2.withColumn('pause', F.lit(None))

    ## define cumulative duration
    df2 = df2.withColumn('cum_pause', F.sum('duration').over(Window.orderBy(ts_name)
                                                             .rowsBetween(Window.unboundedPreceding, 0)
                                                             )
                         )

    ## cumulative distance during a PAUSED state
    df2 = df2.withColumn('pause_dist', F.lit(None))

    w = Window.orderBy(ts_name)

    ## set of conditions for a given state:
    ## TODO: check if it is ok to replace the condition on the duration with one on fixTypeCode=2

    cond0b = (F.col('duration') == 0) & (F.col('cum_dist_min') >= min_dist_per_min) & \
             (F.col('cum_dist_min') <= max_dist_per_min) & (F.col('duration') <= 60)

    cond0c = (F.col('duration') == 0) & \
             ((F.col('cum_dist_min') < min_dist_per_min) | (F.col('cum_dist_min') > max_dist_per_min) |
              (F.col('duration') > 60))

    cond1 = (F.col('cum_dist_min') >= min_dist_per_min) & (F.col('duration') <= min_pause_duration)

    cond2 = (F.lag('state', 1).over(w) == 2) & \
            (F.col('tripType') != 1) & (F.col('tripType') != 4) & (F.col('cum_dist_min') < min_dist_per_min) & \
            (F.col('duration') <= min_pause_duration)

    cond2a = (F.col('pause').isNull()) & (F.lag('state', 1).over(w) == 3) & \
             (F.col('tripType') != 1) & (F.col('tripType') != 4) & (F.lag('tripType', 1).over(w) == 2)

    cond3 = (F.lag('state', 1).over(w) == 2) & (F.col('duration') > max_pause_time)

    cond4 = (F.lag('state', 1).over(w) == 2) & (F.col('duration') > min_pause_duration) & \
            (F.col('duration') <= max_pause_time)

    ## 1. Define MOVING states and MIDPOINT fixes
    df2 = df2.withColumn('state', F.when(cond1, 2).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(cond1, 2).otherwise(F.col('tripType')))

    ## 2. Define last fix as ENDPOINT and state as STATIONARY
    df2 = df2.withColumn('state', F.when(F.col(fix_type_name) == 3, 0).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(F.col(fix_type_name) == 3, 4).otherwise(F.col('tripType')))

    ## 3. Define state and trip type of first tracking point
    df2 = df2.withColumn('state', F.when(cond0b, 2).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(cond0b, 1).otherwise(F.col('tripType')))
    df2 = df2.withColumn('state', F.when(cond0c, 0).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(cond0c, 0).otherwise(F.col('tripType')))

    ## 4. Define ENDPOINT based on pause duration cutoff while MOVING
    df2 = df2.withColumn('state', F.when(cond3, 0).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(cond3, 4).otherwise(F.col('tripType')))

    ## 5. Define PAUSED state based on minimum pause duration while MOVING
    df2 = df2.withColumn('state', F.when(cond4, 3).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(cond4, 3).otherwise(F.col('tripType')))
    df2 = df2.withColumn('pause_dist', F.when(cond4, 0.0).otherwise(F.col('pause_dist')))
    df2 = df2.withColumn('pause', F.when(cond4, F.col('duration')).otherwise(F.col('pause')))

    ## 5. Define PAUSED state while MOVING based on traveled future distance
    df2 = df2.withColumn('state', F.when(cond2, 3).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(cond2, 2).otherwise(F.col('tripType')))
    df2 = df2.withColumn('pause_dist', F.when(cond2a, 0.0).otherwise(F.col('pause_dist')))
    df2 = df2.withColumn('pause', F.when(cond2a, F.col('duration')).otherwise(F.col('pause')))
    df2 = df2.withColumn('state', F.when(F.col('pause').isNotNull(), 3).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(F.col('pause').isNotNull(), 3).otherwise(F.col('tripType')))

    ## 6. Let's define segments at the beginning of a PAUSED state
    df2 = df2.withColumn('segment', F.when(F.col('pause').isNotNull(), F.monotonically_increasing_id() + 1))
    df2 = df2.withColumn('segment', F.when(F.col('segment').isNull(),
                                           F.last('segment', ignorenulls=True)
                                           .over(Window.orderBy(ts_name)
                                                 .rowsBetween(Window.unboundedPreceding, 0)
                                                 )
                                           ).otherwise(F.col('segment'))
                         )
    df2 = df2.withColumn('segment', F.when(F.col('segment').isNull(), 0).otherwise(F.col('segment')))

    ## 7. Partition over a given segment
    w2 = Window.partitionBy('segment').orderBy(ts_name)

    ## 8. Compute the time passed since the beginning of the pause
    df2 = df2.withColumn('pause', F.when(F.col('pause').isNotNull(), F.col('cum_pause') - F.col('duration'))
                         .otherwise(F.col('pause'))
                         )

    df2 = df2.withColumn('pause', F.when((F.col('tripType') != 1) &
                                         (F.col('tripType') != 4),
                                         F.col('cum_pause') - F.last('pause', ignorenulls=True).over(w2)
                                         ).otherwise(F.col('pause'))
                         ).orderBy(ts_name)

    ## 9. Merge segments with duration smaller than max_pause_time
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
                                        F.lag('idx', 1).over(w).isNull(),
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
                                       F.lag('idx', 1).over(w).isNotNull() &
                                       (F.lag('segment', 1).over(w) == F.col('segment') - 1),
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

    ## 10. Recalculate pause-time for merged segments
    df2 = df2.withColumn('pause', F.when(F.col('idx').isNotNull() &
                                         (F.lag('tripType', 1).over(w) == 2) &
                                         (F.lag('segment', 1).over(w) < F.col('segment')),
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

    ## 11. Compute the distance travelled from the beginning of a pause
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

    df2 = df2.withColumn('pause_dist', app_fun(F.col('lat'), F.col('lon'), F.col('lat2'), F.col('lon2')))

    df2 = df2.drop(*['lat2', 'lon2'])

    ## 12. Repartition of the segments into 3 distinct cases

    df2 = df2.drop('idx')

    ### CASE 1
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

    ### CASE 2
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

    ### CASE 3
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

    ## 13. Analyze first segment
    df2 = df2.withColumn('tripType', F.when((F.col('segment') == 0) &
                                            (F.lag('state', 1).over(w) == 0) &
                                            (F.lag('tripType', 1).over(w) == 0) &
                                            (F.col('state') == 2) &
                                            (F.col('tripType') == 2),
                                            1).otherwise(F.col('tripType'))
                         ).orderBy(ts_name)

    ### create a copy of current state and tripType to be used when segments are merged in CASE4
    df2 = df2.withColumn('state_cp', F.col('state'))
    df2 = df2.withColumn('tripType_cp', F.col('tripType'))

    ## 14. Process CASE 1

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

    ct = df2.select('i1', 'duration', 'pause', 'pause_dist').filter(stop).count()
    ct_ = 0
    check = ct + ct_ + ct * ct_
    check_ = 0

    while (check - check_ != 0):
        ct_ = ct
        check_ = check

        df2 = df2.cache()
        df2 = df2.checkpoint()
        df2.count()

        df2 = set_pause(df2, 'i1', ts_name)

        df2 = check_case(df2, 'i1', ts_name, min_dist_per_min, min_pause_duration, max_pause_time)

        df2 = proc_segment(df2, 'j1', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 3, 2, "CASE1")
        df2 = proc_segment(df2, 'j2', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 3, 3, "CASE2")
        df2 = proc_segment(df2, 'j3', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 0, 0, "CASE3")
        df2 = proc_segment(df2, 'j4', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 0, 0, "CASE4")
        df2 = df2.drop(*['j1', 'j2', 'j3', 'j4'])

        ct = df2.select('i1', 'duration', 'pause', 'pause_dist').filter(stop).count()
        check = ct + ct_ + ct * ct_

    ## 15. Process CASE 2

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

    ct = df2.select('i2', 'duration', 'pause', 'pause_dist').filter(stop).count()
    ct_ = 0
    check = ct + ct_ + ct * ct_
    check_ = 0

    while (check - check_ != 0):
        ct_ = ct
        check_ = check

        df2 = df2.cache()
        df2 = df2.checkpoint()
        df2.count()

        df2 = set_pause(df2, 'i2', ts_name)

        df2 = check_case(df2, 'i2', ts_name, min_dist_per_min, min_pause_duration, max_pause_time)

        df2 = proc_segment(df2, 'j1', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 3, 2, "CASE1")
        df2 = proc_segment(df2, 'j2', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 3, 3, "CASE2")
        df2 = proc_segment(df2, 'j3', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 0, 0, "CASE3")
        df2 = proc_segment(df2, 'j4', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 0, 0, "CASE4")
        df2 = df2.drop(*['j1', 'j2', 'j3', 'j4'])

        ct = df2.select('i2', 'duration', 'pause', 'pause_dist').filter(stop).count()
        check = ct + ct_ + ct * ct_

    ## 16. Process CASE 3

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

    ct = df2.select('i3', 'duration', 'pause', 'pause_dist').filter(stop).count()
    ct_ = 0
    check = ct + ct_ + ct * ct_
    check_ = 0

    while (check - check_ != 0):
        ct_ = ct
        check_ = check

        df2 = df2.cache()
        df2 = df2.checkpoint()
        df2.count()

        df2 = set_pause(df2, 'i3', ts_name)

        df2 = check_case(df2, 'i3', ts_name, min_dist_per_min, min_pause_duration, max_pause_time)

        df2 = proc_segment(df2, 'j1', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 3, 2, "CASE1")
        df2 = proc_segment(df2, 'j2', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 3, 3, "CASE2")
        df2 = proc_segment(df2, 'j3', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 0, 0, "CASE3")
        df2 = proc_segment(df2, 'j4', ts_name, min_dist_per_min, min_pause_duration, max_pause_time, 0, 0, "CASE4")
        df2 = df2.drop(*['j1', 'j2', 'j3', 'j4'])

        ct = df2.select('i3', 'duration', 'pause', 'pause_dist').filter(stop).count()
        check = ct + ct_ + ct * ct_

    ## 17. Sanity checks

    ### redefine last fix as ENDPOINT and state as STATIONARY
    df2 = df2.withColumn('state', F.when(F.col(fix_type_name) == 3, 0).otherwise(F.col('state')))
    df2 = df2.withColumn('tripType', F.when(F.col(fix_type_name) == 3, 4).otherwise(F.col('tripType')))

    ### correct isolated points with tripType=1
    df2 = df2.withColumn('tripType', F.when((F.col('state') == 2) &
                                            (F.col('tripType') == 1) &
                                            (F.lag('state', 1).over(w2) == 2) &
                                            (F.lag('tripType', 1).over(w2) == 2) &
                                            (F.lead('state', 1).over(w2) == 2) &
                                            (F.lead('tripType', 1).over(w2) == 2),
                                            2).otherwise(F.col('tripType'))
                         ).orderBy(ts_name)

    ### set trip start after long break
    df2 = df2.withColumn('tripType', F.when((F.col('state') == 2) &
                                            (F.col('tripType') == 2) &
                                            (F.lag('state', 1).over(w2) == 0) &
                                            (F.lag('tripType', 1).over(w2) == 0),
                                            1).otherwise(F.col('tripType'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('tripType', F.when((F.col('state') == 2) &
                                            (F.col('tripType') == 2) &
                                            (F.lag('tripType', 1).over(w2) == 4),
                                            1).otherwise(F.col('tripType'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('tripType', F.when((F.col('state') == 3) &
                                            (F.col('tripType') == 2) &
                                            (F.lead('tripType', 1).over(w2) == 0) &
                                            (F.lead('state', 1).over(w2) == 0),
                                            4).otherwise(F.col('tripType'))
                         ).orderBy(ts_name)

    ### additional checks
    df2 = df2.withColumn('tripType', F.when((F.col('tripType') == 2) &
                                            (F.lag('tripType', 1).over(Window.orderBy(ts_name)) == 0),
                                            1).otherwise(F.col('tripType'))
                         ).orderBy(ts_name)
    df2 = df2.withColumn('tripType', F.when((F.col('tripType') == 2) &
                                            (F.lead('tripType', 1).over(Window.orderBy(ts_name)) == 0),
                                            4).otherwise(F.col('tripType'))
                         ).orderBy(ts_name)

    df2 = df2.drop(*['state_cp', 'tripType_cp', 'total_sec', 'cum_dist_min',
                     'pause', 'pause_dist', 'i1', 'i2', 'i3', 'state', 'segment'])

    df1 = df1.join(df2, [ts_name, 'lat', 'lon', fix_type_name], how='left').orderBy(ts_name)

    """
    ## save dataframe on file
    df1.coalesce(1).write.option("header", True).option("inferSchema", "true") \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv("partial_1.csv")
    """

    return df1


##########################################################################################################

def trip_mode_type(roundSpeed, vehicle_speed_cutoff, bicycle_speed_cutoff, walk_speed_cutoff):
    """
    Estimate trip transportation mode.

    :param roundSpeed:
        fix speed value (in km/h)
    :param vehicle_speed_cutoff:
        speed greater than this value (in Km/h) will be marked as vehicle
    :param bicycle_speed_cutoff:
        speed greater than this value (in Km/h) will be marked as bicycle
    :param walk_speed_cutoff:
        speeds greater than this value (in Km/h) will be marked as pedestrian
    :return:
        integer number classifying trip mode
    """

    # round the speed to the nearest integer
    speed_ = roundSpeed

    try:

        if speed_ < 0:
            value = -1  # unknown trip mode
        # elif speed_ >=0 and speed_ < walk_speed_cutoff:
        #    value = 0  # stationary (not in a trip)
        # elif speed_ >= walk_speed_cutoff and speed_ < bicycle_speed_cutoff:
        elif speed_ >= 0 and speed_ < bicycle_speed_cutoff:
            value = 1  # pedestrian trip
        elif speed_ >= bicycle_speed_cutoff and speed_ < vehicle_speed_cutoff:
            value = 2  # bicycle trip
        elif speed_ >= vehicle_speed_cutoff:
            value = 3  # vehicle trip
        else:
            value = 0

    except:

        value = None

    return value


##########################################################################################################

def trip_segmentation(df, ts_name, speed_segment_length):
    """
    Give trip segmentation for transportation mode classification.

    :param df:
        Spark DataFrame object with timestamp data
    :param ts_name:
        column with timestamp data
    :param speed_segment_length:
        minimum length (in meters) of segments used to classify mode of transportation
    :return:
        Spark DataFrame object with timestamp data
    """

    w1 = Window.orderBy(ts_name).rowsBetween(Window.unboundedPreceding, 0)

    w2 = Window.partitionBy('segment').orderBy(ts_name)

    app_fun = F.udf(lambda a, b, c, d: calc_distance(a, b, c, d))

    ## trip segmentation
    df2 = df.withColumn('segment', F.when(F.col('trip') == 1, F.monotonically_increasing_id()))

    df2 = df2.withColumn('segment', F.when(F.col('segment').isNull() &
                                           F.col('trip').isNotNull(),
                                           F.last('segment', ignorenulls=True).over(w1)
                                           ).otherwise(F.col('segment'))
                         ).orderBy(ts_name)

    ## compute duration and traveled distance for each segment
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
                                              app_fun(F.col('lat'), F.col('lon'), F.col('lat2'), F.col('lon2'))
                                              ).otherwise(F.col('pause_dist'))
                         )
    df2 = df2.drop(*['lat2', 'lon2'])

    ## remove short sub-trips

    df2 = df2.withColumn('t1', F.when((F.col('trip') == 4) &
                                      (F.col('pause_dist') <= speed_segment_length) &
                                      F.lead('trip', 1).over(Window.orderBy(ts_name)).isNotNull(),
                                      F.col('segment')
                                      )
                         )

    df2 = df2.withColumn('t1', F.when(F.col('segment').isNotNull() &
                                      F.col('t1').isNull(),
                                      F.first('t1', ignorenulls=True)
                                      .over(Window.partitionBy('segment')
                                            .rowsBetween(0, Window.unboundedFollowing)
                                            )
                                      ).otherwise(F.col('t1'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t1', F.when(F.col('t1').isNotNull() &
                                      (F.col('trip') == 4) &
                                      F.lead('t1', 1).over(Window.orderBy(ts_name)).isNotNull(),
                                      F.lead('t1', 1).over(Window.orderBy(ts_name))
                                      ).otherwise(F.col('t1'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t2', F.when((F.col('trip') == 4) &
                                      (F.col('pause_dist') <= speed_segment_length) &
                                      F.lead('trip', 1).over(Window.orderBy(ts_name)).isNotNull(),
                                      F.col('pause_dist')
                                      ).otherwise(0.0)
                         )
    df2 = df2.withColumn('t3', F.when(F.col('t1').isNotNull(),
                                      F.sum('t2').over(Window.partitionBy('t1').orderBy(ts_name)
                                                       .rowsBetween(Window.unboundedPreceding, 0)
                                                       )
                                      )
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t2', F.when((F.col('trip') == 4) &
                                      (F.col('t2') == F.col('t3')),
                                      F.col('t2') + F.lag('t3', 1).over(Window.orderBy(ts_name))
                                      ).otherwise(F.col('t2'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t3', F.when(F.col('t1').isNotNull(),
                                      F.sum('t2').over(Window.partitionBy('t1').orderBy(ts_name)
                                                       .rowsBetween(Window.unboundedPreceding, 0)
                                                       )
                                      )
                         ).drop(*['t1', 't2']).orderBy(ts_name)

    df2 = df2.withColumn('trip', F.when((F.col('trip') == 4) &
                                        (F.col('t3') <= speed_segment_length) &
                                        F.lead('trip', 1).over(Window.orderBy(ts_name)).isNotNull(),
                                        F.col('tripType')
                                        ).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('trip', F.when((F.col('trip') == 1) &
                                        (F.lag('t3', 1).over(Window.orderBy(ts_name)) <= speed_segment_length),
                                        F.col('tripType')
                                        ).otherwise(F.col('trip'))
                         ).drop('t3').orderBy(ts_name)

    return df2


##########################################################################################################

def classify_trips(df, ts_name, dist_name, speed_name, fix_type_name, vehicle_speed_cutoff, bicycle_speed_cutoff,
                   walk_speed_cutoff, min_trip_length, min_trip_duration, speed_segment_length, speed_percentile):
    """
    Classify trip transportation mode.

    :param df:
        Spark DataFrame object with timestamp data
    :param ts_name:
        column with timestamp data
    :param dist_name:
        column with distance data
    :param speed_name:
        column with velocity data
    :param fix_type_name:
        column with fix type code
    :param vehicle_speed_cutoff:
        speed greater than this value (in Km/h) will be marked as vehicle
    :param bicycle_speed_cutoff:
         speed greater than this value (in Km/h) will be marked as bicycle
    :param walk_speed_cutoff:
        speeds greater than this value (in Km/h) will be marked as pedestrian
    :param min_trip_length:
        minimum trip length (in meters)
    :param min_trip_duration:
        ,imi,u, trip duration (in meters)
    :param speed_segment_length:
        minimum length (in meters) of segments used to classify mode of transportation
    :param speed_percentile:
         speed comparisons are made at this percentile
    :return:
        Spark DataFrame object with timestamp data
    """

    w1 = Window.orderBy(ts_name).rowsBetween(Window.unboundedPreceding, 0)

    w2 = Window.partitionBy('segment').orderBy(ts_name)

    udf_round = F.udf(lambda x: floor(x + 0.5))  # floor(x+0.5) == Math.round(x) in JavaScript

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

    df2 = df2.withColumn('roundSpeed', F.when(F.col('trip').isNotNull(),
                                              udf_round(F.col(speed_name)).cast(IntegerType())
                                              )
                         )

    df2 = df2.withColumn('trip', F.when((F.col('trip') == 2) &
                                        (F.col('roundSpeed') == 0),
                                        4).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('trip', F.when((F.col('trip') == 4) &
                                        (F.lag('trip', 1).over(Window.orderBy(ts_name)) == 4),
                                        F.col('tripType')
                                        ).otherwise(F.col('trip'))
                         )

    df2 = df2.withColumn('trip', F.when((F.col('tripType') == 2) &  ######## 3
                                        (F.lead('tripType', 1).over(Window.orderBy(ts_name)) == 3),  #####lag 2
                                        4).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('trip', F.when((F.col('trip') == 2) &
                                        (F.lag('trip', 1).over(Window.orderBy(ts_name)) == 4),
                                        1).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('trip', F.when(F.col('trip').isNotNull() &
                                        (F.col('tripType') == 2) &
                                        (F.lag('tripType', 1).over(Window.orderBy(ts_name)) == 3),
                                        1).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('trip', F.when(F.col('trip').isNotNull() &
                                        (F.col('tripType') == 4) &
                                        (F.lead('tripType', 1).over(Window.orderBy(ts_name)) == 0),
                                        4).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    df2 = trip_segmentation(df2, ts_name, speed_segment_length).checkpoint()

    stop = (F.col('trip') == 1)

    ct = df2.filter(stop).count()
    s = ct
    s_ = -1

    while (s - s_ != 0):
        s_ = s

        ct_ = ct

        ## identify segments within a trip
        df2 = trip_segmentation(df2, ts_name, speed_segment_length).checkpoint()

        ct = df2.filter(stop).count()

        s = ct + ct_

    df2 = trip_segmentation(df2, ts_name, speed_segment_length).checkpoint()

    ## set trip mode

    # n_percentile = F.expr('percentile_approx(roundSpeed, {})'.format(str(speed_percentile*0.01)))
    n_percentile = F.expr('percentile(roundSpeed, {})'.format(str(speed_percentile * 0.01)))

    df2b = df2.select(ts_name, 'tripType', 'trip', 'segment', 'roundSpeed')

    df2b = df2b.withColumn('tmp', F.when((F.col('roundSpeed') == 0) &
                                         (F.col('trip') != 4) &
                                         (F.col('tripType') != 0),
                                         0)
                           )
    df2b = df2b.filter(F.col('tmp').isNull()).drop('tmp').orderBy(ts_name)

    df2b = df2b.withColumn('tmp', n_percentile.over(Window.partitionBy('segment'))).orderBy(ts_name)

    df2 = df2.join(df2b, [ts_name, 'tripType', 'trip', 'roundSpeed', 'segment'], how='left').orderBy(ts_name)

    df2 = df2.withColumn('tmp', F.when(F.col('segment').isNotNull() &
                                       F.col('tmp').isNull(),
                                       F.first('tmp', ignorenulls=True)
                                       .over(Window.partitionBy('segment')
                                             .rowsBetween(0, Window.unboundedFollowing)
                                             )
                                       ).otherwise(F.col('tmp'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('tripMOT', F.when(F.col('tmp').isNotNull(),
                                           app_fun(F.col('tmp'))
                                           ).otherwise(F.col('tripMOT'))
                         )

    df2 = df2.withColumn('tripMOT', F.when((F.col('tripType') == 3),
                                           None).otherwise(F.col('tripMOT'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('tripMOT', F.when((F.col('tripType') == 3) &
                                           (F.col('trip') == 4),
                                           F.lag('tripMOT', 1).over(Window.orderBy(ts_name))
                                           ).otherwise(F.col('tripMOT'))
                         ).orderBy(ts_name)

    ## fix repetitions
    df2 = df2.withColumn('trip', F.when((F.col('tripType') == 3) &
                                        (F.col('trip') == 4) &
                                        (F.lag('trip', 1).over(Window.orderBy(ts_name)) == 4),
                                        None
                                        ).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('tripMOT', F.when((F.col('tripType') == 3) &
                                           F.col('trip').isNull() &
                                           F.col('tripMOT').isNotNull(),
                                           None).otherwise(F.col('tripMOT'))
                         )

    df2 = df2.withColumn('trip', F.when((F.col('tripType') == 2) &
                                        (F.lead('tripType', 1).over(Window.orderBy(ts_name)) == 0),
                                        4).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('tripMOT', F.when((F.col('trip') == 1) &
                                           (F.col('tripMOT') == 0),
                                           F.lead('tripMOT', 1).over(Window.orderBy(ts_name))
                                           ).otherwise(F.col('tripMOT'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('trip', F.when((F.col('trip') == 1) &
                                        (F.lag('trip', 1).over(Window.orderBy(ts_name)) == 1),
                                        F.col('tripType')
                                        ).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('trip', F.when((F.col('tripType') != 4) &
                                        (F.col('trip') == 4) &
                                        (F.lead('trip', 1).over(Window.orderBy(ts_name)) == 4),
                                        F.col('tripType')
                                        ).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    df2 = df2.drop(*['segment', 'pause', 'pause_dist', 'tmp']).orderBy(ts_name)

    df3 = df2.select(ts_name, 'lat', 'lon', 'duration', 'distance', 'cum_pause', 'tripType', 'trip', 'tripMOT') \
        .filter(F.col('tripType') != 0).orderBy(ts_name)

    df2 = df2.drop(*['trip', 'tripMOT'])

    df3 = df3.filter(F.col('tripMOT').isNotNull()).orderBy(ts_name)

    df3 = df3.withColumn('ch', F.when(F.col('tripType') == 1, F.monotonically_increasing_id())
                         ).orderBy(ts_name)

    df3 = df3.withColumn('ch', F.when(F.col('ch').isNull() &
                                      (F.col('tripType') != 0),
                                      F.last('ch', ignorenulls=True)
                                      .over(Window.orderBy(ts_name).rowsBetween(Window.unboundedPreceding, 0))
                                      ).otherwise(F.col('ch'))
                         ).orderBy(ts_name)

    ## merge adjacent segments with equal tripMOT
    w4 = Window.partitionBy('ch').orderBy(ts_name)

    df3 = df3.withColumn('trip', F.when((F.col('trip') == 4) &
                                        (F.lead('trip', 1).over(w4) == 1) &
                                        (F.col('tripMOT') == F.lead('tripMOT', 1).over(w4)),
                                        F.col('tripType')
                                        ).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    df3 = df3.withColumn('trip', F.when((F.col('trip') == 1) &
                                        (F.lag('trip', 1).over(w4) == F.lag('tripType', 1).over(w4)) &
                                        (F.col('tripMOT') == F.lag('tripMOT', 1).over(w4)),
                                        F.col('tripType')
                                        ).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    ## trip segmentation
    df3 = trip_segmentation(df3, ts_name, speed_segment_length)

    ## remove short trips
    df3 = df3.withColumn('cum_dist', F.sum(dist_name).over(w4.rowsBetween(Window.unboundedPreceding, 0))
                         ).orderBy(ts_name)

    df3 = df3.withColumn('tmp', F.when((F.col('trip') == 4) &
                                       ((F.col('cum_dist') < min_trip_length) |
                                        (F.col('pause') < min_trip_duration)),
                                       1)
                         ).orderBy(ts_name)

    df3 = df3.withColumn('tmp', F.when(F.col('tmp').isNull(),
                                       F.last('tmp', ignorenulls=True)
                                       .over(w2.rowsBetween(0, Window.unboundedFollowing))
                                       ).otherwise(F.col('tmp'))
                         ).orderBy(ts_name)

    df3 = df3.withColumn('tmp2', F.when((F.col('tmp') == 1) &
                                        (F.col('tripType') == 1),
                                        0)
                         ).orderBy(ts_name)

    df3 = df3.withColumn('tmp2', F.when(F.col('tmp2').isNull(),
                                        F.last('tmp2', ignorenulls=True)
                                        .over(w2.rowsBetween(Window.unboundedPreceding, 0))
                                        ).otherwise(F.col('tmp2'))
                         ).orderBy(ts_name)

    df3 = df3.withColumn('tmp3', F.when((F.col('tmp') == 1) &
                                        F.col('tmp2').isNotNull() &
                                        (F.col('tripType') == 4),
                                        0)
                         ).orderBy(ts_name)

    df3 = df3.withColumn('tmp3', F.when(F.col('tmp3').isNull() &
                                        F.col('tmp2').isNotNull(),
                                        F.last('tmp3', ignorenulls=True)
                                        .over(w2.rowsBetween(0, Window.unboundedFollowing))
                                        ).otherwise(F.col('tmp3'))
                         ).orderBy(ts_name)

    df3 = df3.withColumn('tmp', F.when(F.col('tmp2').isNotNull() &
                                       F.col('tmp3').isNotNull(),
                                       0).otherwise(F.col('tmp'))
                         ).drop(*['tmp2', 'tmp3']).orderBy(ts_name)

    ## reset short isolated trips
    df3 = df3.withColumn('trip', F.when(F.col('tmp') == 0, 0).otherwise(F.col('trip')))

    df3 = df3.withColumn('tripMOT', F.when(F.col('tmp') == 0, 0).otherwise(F.col('tripMOT')))

    df3 = df3.withColumn('tmp', F.when(F.col('tmp') == 0, None).otherwise(F.col('tmp')))

    df3 = df3.withColumn('tmp2', F.when((F.col('tmp') == 1) &
                                        (F.col('tripType') == 1),
                                        2)
                         )

    df3 = df3.withColumn('tmp2', F.when(F.col('tmp2').isNull(),
                                        F.last('tmp2', ignorenulls=True)
                                        .over(w2.rowsBetween(Window.unboundedPreceding, 0))
                                        ).otherwise(F.col('tmp2'))
                         ).orderBy(ts_name)

    df3 = df3.withColumn('trip', F.when(F.col('tmp2') == 2, 0).otherwise(F.col('trip')))

    df3 = df3.withColumn('tripMOT', F.when(F.col('tmp2') == 2, 0).otherwise(F.col('tripMOT')))

    df3 = df3.withColumn('tmp', F.when(F.col('tmp2') == 2, None).otherwise(F.col('tmp')))

    df3 = df3.drop('tmp2')

    ## merge short trip segments
    df3 = df3.withColumn('tripMOT', F.when(F.col('tmp') == 1, None).otherwise(F.col('tripMOT')))

    df3 = df3.withColumn('tripMOT', F.when((F.col('tmp') == 1) &
                                           (F.col('trip') == 1),
                                           F.lag('tripMOT', 1).over(Window.orderBy(ts_name))
                                           ).otherwise(F.col('tripMOT'))
                         ).orderBy(ts_name)

    df3 = df3.withColumn('tripMOT', F.when((F.col('tmp') == 1) &
                                           F.col('tripMOT').isNull(),
                                           F.last('tripMOT', ignorenulls=True)
                                           .over(w2.rowsBetween(Window.unboundedPreceding, 0))
                                           ).otherwise(F.col('tripMOT'))
                         ).orderBy(ts_name)

    df3 = df3.withColumn('trip', F.when((F.col('tmp') == 1) &
                                        (F.col('trip') == 1),
                                        F.col('tripType')
                                        ).otherwise(F.col('trip'))
                         )

    df3 = df3.withColumn('trip', F.when(F.col('tmp').isNull() &
                                        (F.lead('tmp', 1).over(Window.orderBy(ts_name)) == 1) &
                                        (F.col('trip') == 4),
                                        F.col('tripType')
                                        ).otherwise(F.col('trip'))
                         )

    ## merge adjacent segments
    df3 = df3.withColumn('trip', F.when((F.col('trip') == 4) &
                                        (F.lead('trip', 1).over(w4) == 1) &
                                        (F.col('tripMOT') == F.lead('tripMOT', 1).over(w4)),
                                        F.col('tripType')
                                        ).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    df3 = df3.withColumn('trip', F.when((F.col('trip') == 1) &
                                        (F.lag('trip', 1).over(w4) == F.lag('tripType', 1).over(w4)) &
                                        (F.col('tripMOT') == F.lag('tripMOT', 1).over(w4)),
                                        F.col('tripType')
                                        ).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    ## trip segmentation
    df3 = trip_segmentation(df3, ts_name, speed_segment_length)

    df3 = df3.drop('ch').orderBy(ts_name).cache()

    df2 = df2.join(df3, [ts_name, 'lat', 'lon', 'duration', 'distance', 'cum_pause',
                         'tripType'], how='left').orderBy(ts_name)

    df2 = df2.withColumn('tripMOT', F.when((F.col('tripType') == 3) &
                                           (F.col('trip') != 4),
                                           0).otherwise(F.col('tripMOT'))
                         )

    df2 = df2.withColumn('tripMOT', F.when((F.col('tripType') == 3) &
                                           (F.col('trip') == 4),
                                           F.lag('tripMOT', 1).over(Window.orderBy(ts_name))
                                           ).otherwise(F.col('tripMOT'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('tripMOT', F.when(F.col('tripMOT').isNull(), 0).otherwise(F.col('tripMOT')))

    df2 = df2.withColumn('trip', F.when(F.col('trip').isNull(), F.col('tripType')).otherwise(F.col('trip')))

    df3.unpersist()

    df2 = df2.withColumn('trip', F.when((F.col('tmp') == 1) &
                                        (F.col('tripMOT') == 0),
                                        0).otherwise(F.col('trip'))
                         )

    ## sanity checks

    ### set beginning and end of a trip when there is a change of tripMOT
    df2 = df2.withColumn('trip', F.when((F.col('trip') == 2) &
                                        (F.lead('trip', 1).over(Window.orderBy(ts_name)) == 2) &
                                        (F.col('tripMOT') != F.lead('tripMOT', 1).over(Window.orderBy(ts_name))),
                                        4).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('trip', F.when((F.col('trip') == 2) &
                                        (F.lag('trip', 1).over(Window.orderBy(ts_name)) == 4) &
                                        (F.col('tripMOT') != F.lag('tripMOT', 1).over(Window.orderBy(ts_name))),
                                        1).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    ### case of a single fix with tripType=3
    df2 = df2.withColumn('tripMOT', F.when((F.col('trip') == 3) &
                                           (F.lag('trip', 1).over(Window.orderBy(ts_name)) == 2) &
                                           (F.lead('trip', 1).over(Window.orderBy(ts_name)) == 2),
                                           F.lead('tripMOT', 1).over(Window.orderBy(ts_name))
                                           ).otherwise(F.col('tripMOT'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('trip', F.when((F.col('trip') == 3) &
                                        (F.lag('trip', 1).over(Window.orderBy(ts_name)) == 2) &
                                        (F.lead('trip', 1).over(Window.orderBy(ts_name)) == 2),
                                        2).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    ### case of a fix with tripType=0, followed by a fix with tripType=3 and preceded by a fix with tripType=2
    df2 = df2.withColumn('trip', F.when((F.col('trip') == 0) &
                                        (F.lag('trip', 1).over(Window.orderBy(ts_name)) == 2) &
                                        (F.lead('trip', 1).over(Window.orderBy(ts_name)) == 3),
                                        3).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    ### case of last fix which is not the end of a trip
    df2 = df2.withColumn('tripMOT', F.when((F.col(fix_type_name) == 3) &
                                           (F.col('trip') == 4) &
                                           (F.lag('trip', 1).over(Window.orderBy(ts_name)) == 0),
                                           0).otherwise(F.col('tripMOT'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('trip', F.when((F.col(fix_type_name) == 3) &
                                        (F.col('trip') == 4) &
                                        (F.lag('trip', 1).over(Window.orderBy(ts_name)) == 0),
                                        0).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    ### case of first fix followed by start trip
    df2 = df2.withColumn('tripMOT', F.when((F.col(fix_type_name) == 2) &
                                           (F.col('trip') == 0) &
                                           (F.lead('trip', 1).over(Window.orderBy(ts_name)) == 1),
                                           F.lead('tripMOT', 1).over(Window.orderBy(ts_name))
                                           ).otherwise(F.col('tripMOT'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('trip', F.when((F.col(fix_type_name) == 2) &
                                        (F.col('trip') == 0) &
                                        (F.lead('trip', 1).over(Window.orderBy(ts_name)) == 1),
                                        1).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('trip', F.when((F.col('trip') == 1) &
                                        (F.lag('trip').over(Window.orderBy(ts_name)) == 1) &
                                        (F.lag(fix_type_name, 1).over(Window.orderBy(ts_name)) == 2),
                                        2).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    ### case of tripType=2 followed by tripType=0
    df2 = df2.withColumn('trip', F.when((F.col('trip') == 2) &
                                        (F.lead('trip', 1).over(Window.orderBy(ts_name)) == 0),
                                        4).otherwise(F.col('trip'))
                         ).orderBy(ts_name)

    df2 = df2.drop(*['tmp', 'cum_dist', 'roundSpeed', 'pause', 'pause_dist', 'segment'])

    ## compute trip number
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

    ## reset tripType and tripNumber
    df2 = df2.withColumn('tripType', F.col('trip')).orderBy(ts_name)

    df2 = df2.withColumn('tripNumber', F.when((F.col('tripMOT') == 0) &
                                              (F.col('tripType') == 0),
                                              0).otherwise(F.col('tripNumber'))
                         ).orderBy(ts_name)

    ## sanity checks

    ### update trip number for first fixes
    df2 = df2.withColumn('tripNumber', F.when((F.col(fix_type_name) == 2) &
                                              (F.col('tripType') == 1),
                                              F.lead('tripNumber', 1).over(Window.orderBy(ts_name))
                                              ).otherwise(F.col('tripNumber'))
                         ).orderBy(ts_name)

    ### reset trip number of isolated pause fixes
    df2 = df2.withColumn('t1', F.when((F.col('tripType') == 3) &
                                      (F.lag('tripType').over(Window.orderBy(ts_name)) == 0) &
                                      (F.col('tripNumber') != 0),
                                      0)
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t1', F.when((F.col('tripType') == 3) &
                                      (F.lead('tripType').over(Window.orderBy(ts_name)) == 0) &
                                      (F.col('tripNumber') != 0),
                                      0).otherwise(F.col('t1'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t1', F.when((F.col('tripType') == 3) &
                                      (F.lag('tripType').over(Window.orderBy(ts_name)) == 3) &
                                      (F.lag('t1').over(Window.orderBy(ts_name)) == 0) &
                                      (F.col('tripNumber') != 0),
                                      1).otherwise(F.col('t1'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t1', F.when((F.col('tripType') == 3) &
                                      (F.lead('tripType').over(Window.orderBy(ts_name)) == 3) &
                                      (F.lead('t1').over(Window.orderBy(ts_name)) == 0) &
                                      (F.col('tripNumber') != 0),
                                      1).otherwise(F.col('t1'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t1', F.when((F.col('tripType') == 3) &
                                      F.col('t1').isNull(),
                                      F.last('t1', ignorenulls=True)
                                      .over(Window.orderBy(ts_name)
                                            .rowsBetween(Window.unboundedPreceding, 0))
                                      ).otherwise(F.col('t1'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t1', F.when((F.col('t1') == 1) &
                                      (F.lag('t1', 1).over(Window.orderBy(ts_name)) == 0) &
                                      (F.lead('t1', 1).over(Window.orderBy(ts_name)) == 0),
                                      0).otherwise(F.col('t1'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t1', F.when((F.col('tripType') == 3) &
                                      (F.col('t1') == 0) &
                                      (F.lead('t1', 1).over(Window.orderBy(ts_name)) == 1),
                                      1).otherwise(F.col('t1'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t1', F.when((F.col('tripType') == 3) &
                                      (F.col('t1') == 0) &
                                      (F.lag('t1', 1).over(Window.orderBy(ts_name)) == 1),
                                      1).otherwise(F.col('t1'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('tripNumber', F.when((F.col('t1') == 1) &
                                              (F.col('tripType') == 3),
                                              0).otherwise(F.col('tripNumber'))
                         ).drop('t1').orderBy(ts_name)

    ### reset trip number of pause fixes between end trip and start trip
    df2 = df2.withColumn('t2', F.when((F.col('tripType') == 3) &
                                      (F.lag('tripType').over(Window.orderBy(ts_name)) == 4) &
                                      (F.col('tripNumber') != 0),
                                      0)
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t2', F.when((F.col('tripType') == 3) &
                                      (F.lead('tripType').over(Window.orderBy(ts_name)) == 1) &
                                      (F.col('tripNumber') != 0),
                                      0).otherwise(F.col('t2'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t2', F.when((F.col('tripType') == 3) &
                                      (F.lag('tripType').over(Window.orderBy(ts_name)) == 3) &
                                      (F.lag('t2').over(Window.orderBy(ts_name)) == 0) &
                                      (F.col('tripNumber') != 0),
                                      1).otherwise(F.col('t2'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t2', F.when((F.col('tripType') == 3) &
                                      (F.lead('tripType').over(Window.orderBy(ts_name)) == 3) &
                                      (F.lead('t2').over(Window.orderBy(ts_name)) == 0) &
                                      (F.col('tripNumber') != 0),
                                      1).otherwise(F.col('t2'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t2', F.when((F.col('tripType') == 3) &
                                      F.col('t2').isNull(),
                                      F.last('t2', ignorenulls=True)
                                      .over(Window.orderBy(ts_name)
                                            .rowsBetween(Window.unboundedPreceding, 0))
                                      ).otherwise(F.col('t2'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t2', F.when((F.col('tripType') == 3) &
                                      (F.col('t2') == 0) &
                                      (F.lead('t2', 1).over(Window.orderBy(ts_name)) == 1),
                                      1).otherwise(F.col('t2'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t2', F.when((F.col('tripType') == 3) &
                                      (F.col('t2') == 0) &
                                      (F.lag('t2', 1).over(Window.orderBy(ts_name)) == 1),
                                      1).otherwise(F.col('t2'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('tripNumber', F.when((F.col('t2') == 1) &
                                              (F.col('tripType') == 3),
                                              0).otherwise(F.col('tripNumber'))
                         ).orderBy(ts_name)

    ### reset trip number of pause fixes isolated from above
    df2 = df2.withColumn('t1', F.when((F.col('tripType') == 3) &
                                      (F.lag('tripType').over(Window.orderBy(ts_name)) == 0) &
                                      (F.col('tripNumber') != 0),
                                      0)
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t1', F.when((F.col('tripType') == 3) &
                                      (F.lead('tripType').over(Window.orderBy(ts_name)) == 1) &
                                      (F.col('tripNumber') != 0),
                                      0).otherwise(F.col('t1'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t1', F.when((F.col('tripType') == 3) &
                                      (F.lag('tripType').over(Window.orderBy(ts_name)) == 3) &
                                      (F.lag('t1').over(Window.orderBy(ts_name)) == 0) &
                                      (F.col('tripNumber') != 0),
                                      1).otherwise(F.col('t1'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t1', F.when((F.col('tripType') == 3) &
                                      (F.lead('tripType').over(Window.orderBy(ts_name)) == 3) &
                                      (F.lead('t1').over(Window.orderBy(ts_name)) == 0) &
                                      (F.col('tripNumber') != 0),
                                      1).otherwise(F.col('t1'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('t1', F.when((F.col('tripType') == 3) &
                                      F.col('t1').isNull(),
                                      F.last('t1', ignorenulls=True)
                                      .over(Window.orderBy(F.col('t1'))
                                            .rowsBetween(Window.unboundedPreceding, 0))
                                      ).otherwise(F.col('t1'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('tripNumber', F.when((F.col('t1') == 1) &
                                              (F.col('t2') != 1) &
                                              (F.col('tripType') == 3),
                                              0).otherwise(F.col('tripNumber'))
                         ).drop(*['t1', 't2']).orderBy(ts_name)

    ### reset trip without endpoint before a pause fix
    df2 = df2.withColumn('tmp', F.when((F.col('tripType') == 2) &
                                       (F.lead('tripType', 1).over(Window.orderBy(ts_name)) == 3) &
                                       (F.lead('tripNumber', 1).over(Window.orderBy(ts_name)) == 0),
                                       1)
                         ).orderBy(ts_name)

    df2 = df2.withColumn('tmp', F.when(F.col('tmp').isNull(),
                                       F.last('tmp', ignorenulls=True)
                                       .over(Window.partitionBy('tripNumber')
                                             .orderBy(ts_name).rowsBetween(0, Window.unboundedFollowing)
                                             )
                                       ).otherwise(F.col('tmp'))
                         ).orderBy(ts_name)

    df2 = df2.withColumn('tripType', F.when(F.col('tmp').isNotNull(),
                                            0).otherwise(F.col('trip'))
                         )

    df2 = df2.withColumn('tripMOT', F.when(F.col('tmp').isNotNull(),
                                           0).otherwise(F.col('tripMOT'))
                         )

    df2 = df2.withColumn('tripNumber', F.when(F.col('tmp').isNotNull(),
                                              0).otherwise(F.col('tripNumber'))
                         ).drop('tmp').orderBy(ts_name)
    """
    ## recompute trip number
    df2 = df2.withColumn('tripNumber', F.when(F.col('tripType') == 1, F.monotonically_increasing_id()))
    df2 = df2.withColumn('tripNumber', F.when(F.col('tripNumber').isNull() &
                                              F.col('tripType').isNotNull(),
                                              F.last('tripNumber', ignorenulls=True).over(w1)
                                              ).otherwise(F.col('tripNumber'))
                         ).orderBy(ts_name)
    df2 = df2.withColumn('tripNumber', F.when(F.col('tripNumber').isNotNull(),
                                              F.col('tripNumber') + F.lit(1)
                                              ).otherwise(F.col('tripNumber'))
                         )
    df2 = df2.withColumn('tripNumber', F.when(F.col('tripType') == 0, 0).otherwise(F.col('tripNumber')))
    df2 = df2.withColumn('tripNumber', F.when(F.col('tripNumber').isNull(), 0).otherwise(F.col('tripNumber')))
    """
    df2 = df2.select(ts_name, 'dow', 'lat', 'lon', fix_type_name, 'tripNumber', 'tripType', 'tripMOT')

    """
    ## save results
    df2.coalesce(1).write.option("header", True).option("inferSchema", "true") \
                   .option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv("partial_2.csv")
    """
    return df2

##########################################################################################################
