"""
MIT License

Copyright (c) 2021 Emiliano Molinaro

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
from datetime import datetime, timedelta
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from pyspark.sql.types import IntegerType
import datefinder
import time

##########################################################################################################

def queryrows(df, string):
    """
    Select the rows which match a given string.

    :param df:
        Spark DataFrame object containing the accelerometer raw data
    :param string:
        matching string
    :return:
        Spark DataFrame object with filtered values
    """

    filter_value = df.schema.names[0] + " like '%" + string + "%'"

    return df.filter(filter_value).collect()[0][0]


##########################################################################################################

def gen_acc_dataframe(df, ts_name):
    """
    Generate accelerometer DataFrame from raw data with timestamp column.

    :param df:
        Spark DataFrame object containing the accelerometer raw data
    :param ts_name:
        name of timestamp column
    :return:
        input data epoch, Spark DataFrame object with timestamp data
    """

    ## extract metadata from RDD object
    start_date = queryrows(df, 'Start Date').split()[2]
    start_time = queryrows(df, 'Start Time').split()[2]
    interval = queryrows(df, 'Period').split()[3]
    dateformat = queryrows(df, 'Data File Created By').split()

    if len(dateformat) < 14:

        dt = datefinder.find_dates(start_date + " " + start_time)
        start_timestamp = [ts for ts in dt][0]

    else:

        date_dict = {'M/d/yyyy': ['%m/%d/%Y', '%m/%d/%Y'],
                     'dd-MMM-yy': ['%d/%m/%Y', '%d-%b-%y'],
                     'dd-MM-yyyy': ['%d/%m/%Y', '%d-%m-%Y'],
                     'dd/MM/yyyy': ['%d/%m/%Y', '%d/%m/%Y']
                     }

        dt = dateformat[13]
        datetime_format = [date_dict[dt][0] + ' %H:%M:%S', date_dict[dt][1] + ' %H:%M:%S']
        start_timestamp = datetime.strptime(start_date + " " + start_time, datetime_format[0])

    x = time.strptime(interval, '%H:%M:%S')

    interval_sec = timedelta(hours=x.tm_hour, minutes=x.tm_min, seconds=x.tm_sec)

    ## extract accelerometer data from RDD object
    acc_data = df.filter("not value like '%Current%'")
    acc_data = acc_data.filter("not value like '%Axis%'")
    acc_data = acc_data.filter("value like '%,%'")  # TODO: change 'value' as 'df.schema.names[0]'
    acc_data = acc_data.cache()
    acc_data = acc_data.checkpoint()
    acc_data.count()
    acc_data = acc_data.selectExpr('value as acc_data')  # change column name to 'acc_data'
    acc_data = acc_data.withColumn('id', F.monotonically_increasing_id())

    app_fun = F.udf(lambda k: start_timestamp + k * interval_sec, TimestampType())

    acc_data = acc_data.withColumn(ts_name, app_fun(acc_data['id'])).select([ts_name, 'acc_data'])

    return interval_sec.seconds, acc_data


##########################################################################################################

def split_acc_data(df, col_list):
    """
    Reads string of data from accelerometer DataFrame.

    :param df:
        Spark DataFrame object with timestamp data
    :param col_list:
        list of feature names matching accelerometer data
    :return:
        Spark DataFrame object with timestamp data
    """

    df = df.cache()
    df = df.checkpoint()
    df.count()

    cols = F.split(df['acc_data'], r',')

    for k, item in enumerate(col_list):
        df = df.withColumn(item, cols.getItem(k).cast(dataType=IntegerType()))

    df = df.drop('acc_data')

    return df


##########################################################################################################

def activity_count(df, datetime_col, interval, LightCO, ModerateCO, HardCO, VeryHardCO, incl_acc=False):
    """
    Return activity count calculated from accelerometer data.

    :param df:
        Spark DataFrame object with timestamp data
    :param datetime_col:
        column with timestamp data
    :param interval:
        epoch duration (in seconds)
    :param LightCO:
        light activity cutoff value
    :param ModerateCO:
        moderate activity cutoff value
    :param HardCO:
        hard activity cutoff value
    :param VeryHardCO:
        very hard activity cutoff value
    :param incl_acc:
        if true, all raw accelerometer data are included in the DataFrame
    :return:
        Spark DataFrame object with timestamp data
    """

    df = df.cache()
    df = df.checkpoint()
    df.count()

    cols = df.columns

    app_fun = F.udf(lambda x: activity_index(x, interval, LightCO, ModerateCO, HardCO, VeryHardCO))

    ## use the axis1 or vectMag to determine the activity count
    df = df.withColumn('activity', F.col(cols[1]))

    df = df.withColumn('activityIntensity', app_fun(df['activity'])).orderBy(datetime_col)

    cols.insert(1, 'activity')

    cols.insert(2, 'activityIntensity')

    if not incl_acc:

        df = df.select(cols[0:3]).orderBy(datetime_col)

    else:

        df = df.select(cols).orderBy(datetime_col)

    return df


##########################################################################################################

def activity_index(AC, interval, LightCO, ModerateCO, HardCO, VeryHardCO):
    """
    Calculate activity intensity level using Freedson adult cut points (Freedson, Melanson, & Sirard, 1998).

    :param AC:
        activity count per epoch
    :param interval:
        epoch duration (in seconds)
    :param LightCO:
        light activity cutoff value
    :param ModerateCO:
        moderate activity cutoff value
    :param HardCO:
        hard activity cutoff value
    :param VeryHardCO:
        very hard activity cutoff value
    :return:
        integer value corresponding to the activity intensity
    """

    ## assume epoch smaller than 1 minute
    assert interval <= 60, "Epoch larger than 1 minute."

    ## normalize the cutoffs per epoch
    n = 60 / interval
    VeryHardCO = VeryHardCO / n
    HardCO = HardCO / n
    ModerateCO = ModerateCO / n
    LightCO = LightCO / n

    if AC == -1:
        act_index = -1  # state unknown
    elif AC == -2:
        act_index = -2  # not wearing device
    elif 0 <= AC < LightCO:
        act_index = 0  # sedentary
    elif LightCO <= AC < ModerateCO:
        act_index = 1  # light activity
    elif ModerateCO <= AC < HardCO:
        act_index = 2  # moderate activity
    elif HardCO <= AC < VeryHardCO:
        act_index = 3  # hard activity
    else:
        act_index = 4  # very hard activity

    return act_index


##########################################################################################################
### NOT USED ###
def datetime_filter(df, param_name, param_value, datetime_name, time_w=90, step=90 * 60):
    """
    Remove rows in DataFrame which match a condition within a given time interval.

    :param df:
        Spark DataFrame object with timestamp data
    :param param_name:
        parameter name
    :param param_value:
        parameter value for conditional statement
    :param datetime_name:
        column with timestamp data
    :param time_w:
        tumbling window duration (in minutes)
    :param step:
        sliding interval (in seconds)
    :return:
        Spark DataFrame object with timestamp data
    """

    ## tumbling window size
    tw = str(time_w) + ' minutes'

    ## sliding window size
    sw = str(step) + ' seconds'

    ## offset (in seconds)
    offset = str(0) + ' seconds'

    intervals_df = df.groupBy(F.window(datetime_name, '{}'.format(tw), '{}'.format(sw), '{}'.format(offset))) \
        .avg(param_name) \
        .sort('window.start') \
        .filter(F.col('avg({})'.format(param_name)) == param_value) \
        .select('window') \
        .withColumn('start', F.col('window').start) \
        .withColumn('end', F.col('window').end) \
        .drop('window')

    """
        schema of internal_df:
        
        root
         |-- start: timestamp (nullable = true)
         |-- end: timestamp (nullable = true)
    """

    ## transform dataframe into list of pyspark.sql.types.Row objects
    intervals_list = intervals_df.collect()

    ## filter dataframe excluding the selected intervals
    for row in intervals_list:
        df = df.filter(~F.col(datetime_name).between(row[0], row[1]))

    return intervals_df, df


##########################################################################################################
### NOT USED ###
def start_time_offset(df):
    """
    Return the offset to start a tumbling window from the first timestamp of the DataFrame.

    :param df:
        Spark DataFrame object with timestamp data
    :return:
        number of seconds
    """

    ## notice: the resulting offset must be smaller than the tumbling window
    st_date = df.first()[0]
    st_min = st_date.minute
    st_sec = st_date.second
    start_time = (st_min - 10 * (st_min // 10)) * 60 + st_sec
    offset = '{} seconds'.format(str(start_time))

    return offset


##########################################################################################################

def consecutive_time(df, ts_name, interval):
    """
    Add two columns with the start date and end date of consecutive timestamps which differ by a given interval.

    :param df:
        Spark DataFrame object with timestamp data
    :param ts_name:
        column with timestamp data
    :param interval:
        required precision (in seconds)
    :return:
        Spark DataFrame object with timestamp data
    """

    df_ = df.withColumn("rn", F.row_number().over(Window.orderBy('{}'.format(ts_name))))

    df_ = df_.cache()
    df_ = df_.checkpoint()
    df_.count()

    df_.createOrReplaceTempView('df_')

    spark = SparkSession.builder.getOrCreate()

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

def detect_bouts(df, ts_name, col_name, new_col, interval, UP, LOW, DURATION, TOL):
    """
    Create a new column based on filters on timestamps.

    :param df:
        Spark DataFrame object with timestamp data
    :param ts_name:
        column with timestamp data
    :param col_name:
        column on which the filter must be applied
    :param new_col:
        column with applied filters
    :param interval:
        epoch duration (in seconds)
    :param UP:
        upper limit of activity count per minute
    :param LOW:
        lower limit of activity count per minute
    :param DURATION:
        minimum bout duration (in minutes)
    :param TOL:
        tolerance (in minutes)
    :return:
        Spark DataFrame object with timestamp data
    """

    ## assume one epoch smaller than 1 minute
    assert interval <= 60, "Epoch larger than 1 minute."

    ## number of epochs per minute
    n = 60 / interval

    ## bounds on measured quantitity per epoch
    up, low = (UP / n, LOW / n)

    ## convert tolerance in seconds
    tol = TOL * 60

    ## number of epoch in tolerance interval
    epochs_tol = tol / interval

    ## convert minimum bout duration in seconds
    duration = DURATION * 60

    ## Number of epochs in minimum bout duration
    epochs_min_bout = duration / interval

    ## filter dataframe with:   low <= col_name <= up
    inbout = (F.col('{}'.format(col_name)) >= low) & (F.col('{}'.format(col_name)) <= up)
    df1 = df.filter(inbout).orderBy('{}'.format(ts_name))
    df1.checkpoint()

    ## determine consecutive timestamps in df1
    df1 = consecutive_time(df1, '{}'.format(ts_name), interval)
    df1 = df1.selectExpr(['{}'.format(ts_name), 'start as activity_start'])

    pause_list = 0

    if TOL > 0:
        ## filter data with col_name < low and col_name > up
        df2 = df.filter(~inbout).orderBy('{}'.format(ts_name))
        df2 = df2.cache()
        df2 = df2.checkpoint()
        df2.count()

        ## determine consecutive timestamps in df2
        df2 = consecutive_time(df2, '{}'.format(ts_name), interval)
        df2 = df2.selectExpr(['{}'.format(ts_name), 'start as pause_start'])

        ## filter periods larger than tolerance
        df2 = df2.groupBy('pause_start').count()
        df2 = df2.filter(df2['count'] > epochs_tol).orderBy('pause_start')
        df2 = df2.withColumn('pause_end', (F.col('pause_start').cast(IntegerType()) +
                                           (F.col('count') - 1) * interval).cast(TimestampType())
                             ).drop('count')

        pause_list = df2.collect()

    ## merge df1 to the accelerometer dataframe
    df3 = df.join(df1, ['{}'.format(ts_name)], 'leftouter')
    df3.checkpoint()

    if TOL > 0:

        ## assign pause periods
        df3 = df3.withColumn('pause', F.lit(0))

        for row in pause_list:
            df3 = df3.withColumn('pause', F.when((F.col('{}'.format(ts_name)) >= row['pause_start']) &
                                                 (F.col('{}'.format(ts_name)) <= row['pause_end']),
                                                 1).otherwise(F.col('pause'))
                                 )

        ## assign previous non-zero 'start' to missing values given 'pause' < tolerance
        df3 = df3.withColumn('activity_start', F.when((F.col('activity_start').isNull()) &
                                                      (F.col('pause') == 0),
                                                      F.last(F.col('activity_start'), ignorenulls=True)
                                                      .over(Window.orderBy(ts_name))
                                                      ).otherwise(F.col('activity_start'))
                             ).drop('pause')

    ## define a flag to select rows with non-zero 'activity_start'
    df3 = df3.withColumn('check', F.when(F.col('activity_start').isNotNull(), F.lit(1)).otherwise(F.lit(0)))

    ## select rows with non-zero 'activity_start'
    df2 = df3.select(['{}'.format(ts_name), 'check']).filter(F.col('check') == 1)

    ## assign bout start
    df2 = consecutive_time(df2, '{}'.format(ts_name), interval).selectExpr(
        ['{}'.format(ts_name), 'start as bout_start'])

    ## assign bout to dataframe
    df3 = df3.join(df2, ['{}'.format(ts_name)], 'leftouter').drop(*['activity_start', 'check'])

    df3 = df3.withColumn('bout_start', F.when(F.col('bout_start').isNull(),
                                              F.col(ts_name)
                                              ).otherwise(F.col('bout_start'))
                         )

    ## filter periods larger than the minimum bout duration
    df1 = df3.groupBy('bout_start').count()
    df1.checkpoint()

    df1 = df1.filter(df1['count'] > epochs_min_bout).orderBy('bout_start')

    df1 = df1.withColumn('bout_end', (F.col('bout_start').cast(IntegerType()) +
                                      F.col('count') * interval).cast(TimestampType())
                         ).drop('count')

    df1 = df1.withColumn(new_col, F.row_number().over(Window.orderBy('bout_start')))

    bouts_list = df1.collect()

    ## initialize activityBoutNumber to zero
    df3 = df3.drop(*['start', 'end', 'check', 'pause', 'bout_start'])

    df3 = df3.withColumn(new_col, F.lit(0))

    df3.checkpoint()

    ## assign activityBoutNumber
    for row in bouts_list:
        df3 = df3.withColumn(new_col, F.when((F.col('{}'.format(ts_name)) >= row['bout_start']) &
                                             (F.col('{}'.format(ts_name)) <= row['bout_end']),
                                             row[new_col]
                                             ).otherwise(F.col(new_col))
                             )

    df3 = df3.orderBy('{}'.format(ts_name))

    return df3


##########################################################################################################

def select_acc_intervals(df, ts_name, interval, window, incl_vect=False, incl_acc=False):
    """
    Filter DataFrame with a new epoch duration.

    :param df:
        Spark DataFrame object with timestamp data
    :param ts_name:
        column with timestamp data
    :param interval:
        initial epoch duration (in seconds)
    :param window:
        new epoch duration (in seconds)
    :param incl_vect:
        if true, calculate vector magnitude and include it in the DataFrame
    :param incl_acc:
        if true, all raw accelerometer data are included in the DataFrame
    :return:
        Spark DataFrame object with timestamp data
    """

    ## the window must be larger tha a single epoch
    assert interval <= 60, "Epoch larger than 1 minute."
    assert window >= interval, "Window smaller than epoch."

    cols = df.columns
    selected_cols = ['axis1', 'axis2', 'axis3', 'steps']  # TODO: add eeAccumulator

    minp = df.select(F.min(ts_name).cast('long')).first()[0]

    if interval < window:

        df2 = df.withColumn('tmp', F.row_number().over(Window.orderBy(ts_name)) - 1)

        df2 = df2.withColumn('total_sec', F.col(ts_name).cast('long')).cache()
        df2 = df2.checkpoint()
        df2.count()

        for col in selected_cols:

            df2 = df2.withColumn(col, F.when(((F.col('total_sec') - minp) % window == 0),
                                             F.sum(col).over(Window.orderBy('total_sec')
                                                             .rangeBetween(0, window - interval)
                                                             )
                                             ).otherwise(0)
                                 )

        df2 = df2.withColumn('duration', F.col(ts_name).cast(IntegerType()) -
                             F.lag(F.col(ts_name).cast(IntegerType()), 1, minp)
                             .over(Window.orderBy(ts_name))
                             ).drop('total_sec')

        df2 = df2.withColumn('tmp', (F.col('tmp') * F.col('duration')) % window).drop('duration').orderBy(ts_name)

        df2 = df2.filter(F.col('tmp') == 0).drop('tmp').orderBy(ts_name)

    else:

        df2 = df

    if incl_vect:

        df2 = df2.withColumn('vectMag', F.sqrt(F.col('axis1') ** 2 + F.col('axis2') ** 2 + F.col('axis3') ** 2))

        cols.insert(1, 'vectMag')

        df2 = df2.select(cols).orderBy(ts_name)

    if not incl_acc:

        df2 = df2.select(ts_name, cols[1])

    return df2


##########################################################################################################

def non_wear_filter(df, ts_name, AC_name, AI_name, interval, DURATION):
    """
    Determine non-wearing period, for which activity count and activity intensity are assigned equal to -2.

    :param df:
        Spark DataFrame object with timestamp data
    :param ts_name:
        column with timestamp data
    :param AC_name:
        column with activity count  values
    :param AI_name:
        column with activity intensity values
    :param interval:
        epoch duration (in seconds)
    :param DURATION:
        non-wearing period (in seconds)
    :return:
        Spark DataFrame object with timestamp data
    """

    ## select valid epochs with non-negative activity count
    df1 = df.filter(F.col(AC_name) >= 0)

    df1 = df1.cache()
    df1 = df1.checkpoint()
    df1.count()

    TOL = 0

    UP = 0

    LOW = 0

    new_col = 'no_wear'

    df1 = detect_bouts(df, ts_name, AC_name, new_col, interval, UP, LOW, DURATION, TOL)

    df1 = df1.select([ts_name, new_col])

    ## merge new column with the DataFrame and assing zero to missing values
    df2 = df.join(df1, [ts_name], 'leftouter').orderBy(ts_name).fillna(0, subset=[new_col])

    df2 = df2.cache()
    df2 = df2.checkpoint()
    df2.count()

    ## assign activity count and activity intensity equal to -2 for non valid data
    df2 = df2.withColumn(AC_name, F.when(F.col(new_col) > 0, -2).otherwise(F.col(AC_name)))

    df2 = df2.withColumn(AI_name, F.when(F.col(new_col) > 0, -2).otherwise(F.col(AI_name))).drop(new_col)

    return df2


##########################################################################################################

def activity_bout_filter(df, ts_name, AC_name, new_col, interval, UP, LOW, DURATION, TOL):
    """
    Detect activity bouts.

    :param df:
        Spark DataFrame object with timestamp data
    :param ts_name:
         column with timestamp data
    :param AC_name:
        column with activity count  values
    :param new_col:
        column with applied filters
    :param interval:
        epoch duration (in seconds)
    :param UP:
        upper limit of activity count per minute
    :param LOW:
        lower limit of activity count per minute
    :param DURATION:
        minimum bout duration (in minutes)
    :param TOL:
        tolerance (in minutes)
    :return:
        Spark DataFrame object with timestamp data
    """

    ## select valid epochs with non-negative activity count
    df1 = df.filter(F.col(AC_name) >= 0)

    df1 = df1.cache()
    df1 = df1.checkpoint()
    df1.count()

    df1 = detect_bouts(df, ts_name, AC_name, new_col, interval, UP, LOW, DURATION, TOL)

    df1 = df1.select([ts_name, new_col])

    ## merge new column with the dataframe and assing zero to missing values
    df2 = df.join(df1, [ts_name], 'leftouter').orderBy(ts_name).fillna(0, subset=[new_col])

    return df2


##########################################################################################################

def sedentary_bout_filter(df, ts_name, AC_name, new_col, interval, UP, LOW, DURATION, TOL):
    """
    Detect sedentary bouts.

    :param df:
        Spark DataFrame object with timestamp data
    :param ts_name:
         column with timestamp data
    :param AC_name:
        column with activity count  values
    :param new_col:
        column with applied filters
    :param interval:
        epoch duration (in seconds)
    :param UP:
        upper limit of activity count per minute
    :param LOW:
        lower limit of activity count per minute
    :param DURATION:
        minimum bout duration (in minutes)
    :param TOL:
        tolerance (in minutes)
    :return:
        Spark DataFrame object with timestamp data
    """

    ## select valid epochs with non-negative activity count
    df1 = df.filter(F.col(AC_name) >= 0)

    df1 = df1.cache()
    df1 = df1.checkpoint()
    df1.count()

    df1 = detect_bouts(df, ts_name, AC_name, new_col, interval, UP, LOW, DURATION, TOL)

    df1 = df1.select([ts_name, new_col])

    ## merge new column with the dataframe and assing zero to missing values
    df2 = df.join(df1, [ts_name], 'leftouter').orderBy(ts_name).fillna(0, subset=[new_col])

    return df2

##########################################################################################################
