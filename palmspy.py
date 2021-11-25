#!/usr/bin/env python
# coding: utf-8

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

from pyspark.conf import SparkConf
import textwrap
import json
import sys, os

from src.tools import Input, Metadata
import shutil
import glob

from src.help_menu import parser
from src.GPSProcessing import *
from src.AccProcessing import *

meta = Metadata()

def header():
    version = meta.version
    program = meta.name
    print(" ")
    header = """
    ==========================
    {} v{} 
    ==========================
    """.format(program, version)

    list = textwrap.wrap(header, width=30)
    for element in list:
        print(element)
    print(" ")
    pass

def main(gps_path, acc_path, config_file,
         interval, insert_missing, insert_until,
         insert_max_seconds, los_max_duration, filter_invalid_values,
         max_speed, max_ele_change, min_change_3_fixes,
         detect_trip, min_distance, min_trip_length, min_trip_duration,
         min_pause_duration, max_pause_duration, detect_trip_mode,
         vehicle_cutoff, bicycle_cutoff, walk_cutoff,
         percentile_to_sample, min_segment_length,
         include_acc, include_vect,
         mark_not_wearing_time, minutes_zeros_row,
         detect_activity_bouts, activity_bout_duration,
         activity_bout_upper_limit, activity_bout_lower_limit, activity_bout_tolerance,
         detect_sedentary_bouts, sedentary_bout_duration,
         sedentary_bout_upper_limit, sedentary_bout_tolerance,
         very_hard_cutoff, hard_cutoff, moderate_cutoff, light_cutoff,
         merge_data_to_gps, merge_data_to_acc,
         num_cores, driver_mem, executor_mem, mem_fraction, default_partitions, shuffle_partitions, mem_offHeap_enabled,
         mem_offHeap_size, clean_checkpoints, codegen_wholeStage, codegen_fallback,
         broadcast_timeout, network_timeout):
    program_start = time.time()

    if gps_path is None:
        print("Specify path to GPS data folder.\n")
        sys.exit()

    if acc_path is None:
        print("Specify path to accelerator data folder.\n")
        sys.exit()

    if gps_path[-1] != "/":
        gps_path = gps_path + "/"

    if acc_path[-1] != "/":
        acc_path = acc_path + "/"

    ## Output directory
    if not os.path.exists('PALMSpy_output'):
        os.makedirs('PALMSpy_output')

    ## Load default configuration parameters and update them from command line
    params = Input()
    params.update(interval, insert_missing, insert_until,
                  insert_max_seconds, los_max_duration, filter_invalid_values,
                  max_speed, max_ele_change, min_change_3_fixes,
                  detect_trip, min_distance, min_trip_length, min_trip_duration,
                  min_pause_duration, max_pause_duration, detect_trip_mode,
                  vehicle_cutoff, bicycle_cutoff, walk_cutoff,
                  percentile_to_sample, min_segment_length,
                  include_acc, include_vect,
                  mark_not_wearing_time, minutes_zeros_row,
                  detect_activity_bouts, activity_bout_duration,
                  activity_bout_upper_limit, activity_bout_lower_limit, activity_bout_tolerance,
                  detect_sedentary_bouts, sedentary_bout_duration,
                  sedentary_bout_upper_limit, sedentary_bout_tolerance,
                  very_hard_cutoff, hard_cutoff, moderate_cutoff, light_cutoff,
                  merge_data_to_gps, merge_data_to_acc,
                  num_cores, driver_mem, executor_mem, mem_fraction, default_partitions, shuffle_partitions,
                  mem_offHeap_enabled,
                  mem_offHeap_size, clean_checkpoints, codegen_wholeStage, codegen_fallback,
                  broadcast_timeout, network_timeout)
    settings = params.dump_dict()

    ## Overwrite configuration parameters from file or save them to file
    if config_file == "":
        with open('PALMSpy_output/settings.json', 'w') as f:
            json.dump(settings, f)
    else:
        file_settings = open(config_file, "r")
        with file_settings as f:
            settings = json.load(f)
        ## copy JSON config file into output folder
        shutil.copy(config_file, 'PALMSpy_output/')
        num_cores = settings['spark']['default']['cores']
        default_partitions = settings['spark']['default']['parallelism']

    ## Set Spark configuration parameters
    conf = SparkConf().setAll([('spark.memory.fraction', settings['spark']['memory']['fraction']),
                               ('spark.executor.memory', settings['spark']['executor']['memory']),
                               ('spark.driver.memory', settings['spark']['driver']['memory']),
                               ('spark.sql.shuffle.partitions', settings['spark']['sql']['shuffle']['partitions']),
                               ('spark.memory.offHeap.enabled', settings['spark']['memory']['offHeap']['enabled']),
                               ('spark.memory.offHeap.size', settings['spark']['memory']['offHeap']['size']),
                               ('spark.cleaner.referenceTracking.cleanCheckpoints',
                                settings['spark']['cleaner']['referenceTracking']['cleanCheckpoints']),
                               ('spark.sql.codegen.wholeStage', settings['spark']['sql']['codegen']['wholeStage']),
                               ('spark.sql.broadcastTimeout', settings['spark']['sql']['broadcastTimeout']),
                               ('spark.network.timeout', settings['spark']['network']['timeout']),
                               ('spark.sql.codegen.fallback', settings['spark']['sql']['codegen']['fallback']),
                               ('spark.default.parallelism', settings['spark']['default']['parallelism']),
                               ('spark.scheduler.listenerbus.eventqueue.capacity', '50000'),
                               ('spark.ui.showConsoleProgress', 'false'),
                               ('spark.cleaner.referenceTracking.blocking', 'false'),
                               ('spark.cleaner.periodicGC.interval', '3min')
                               ]
                              )
    ## ('spark.driver.host', 'localhost') # TODO: allows to pass configs with spark-submit, without spark.conf file
    ## ('spark.ui.port', '3000')

    """
    Note about 'spark.driver.memory': 
    In client mode, this config must not be set through the SparkConf directly in your application, 
    because the driver JVM has already started at that point (using spark-submit to run the application). 
    Instead, please set this through the --driver-memory command line option or in your default properties file.
    
    """

    mode = "local[" + str(num_cores) + "]"
    spark = SparkSession.builder.config(conf=conf).master(mode).appName("PALMSpy").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir('checkpoints')
    sc.getConf().getAll()

    spark.catalog.clearCache()

    header()

    print("Maximum number of cores used in the calculation: {}\n".format(str(num_cores)))
    print("Default number of partitions: {}".format(str(default_partitions)))

    ## List accelerometer and GPS data files have the same name
    list_file_acc = sorted(os.listdir(acc_path))
    list_file_gps = sorted(os.listdir(gps_path))
    list_combined = list(zip(list_file_acc, list_file_gps))

    output_filename = 'palmspy'

    ## GPS parameters
    #################

    ### general
    interval = settings['gps']['parameters']['general']['interval']
    insert_missing = settings['gps']['parameters']['general']['insert_missing']
    insert_until = settings['gps']['parameters']['general']['insert_until']
    insert_max_seconds = settings['gps']['parameters']['general']['insert_max_seconds']
    los_max_duration = settings['gps']['parameters']['general']['los_max_duration']

    ### filtering
    filter_invalid_values = settings['gps']['parameters']['filter_options']['filter_invalid_values']
    remove_lone_fixes = settings['gps']['parameters']['filter_options'][
        'remove_lone_fixes']  # TODO: add option to remove lone fixes (default : True)
    max_speed = settings['gps']['parameters']['filter_options']['max_speed']
    max_ele_change = settings['gps']['parameters']['filter_options']['max_ele_change']
    min_change_3_fixes = settings['gps']['parameters']['filter_options']['min_change_3_fixes']

    ### trip detection
    trip_detection = settings['gps']['parameters']['trip_detection']['detect_trip']
    min_distance = settings['gps']['parameters']['trip_detection']['min_distance']
    min_trip_length = settings['gps']['parameters']['trip_detection']['min_trip_length']
    min_trip_duration = settings['gps']['parameters']['trip_detection']['min_trip_duration']
    min_pause_duration = settings['gps']['parameters']['trip_detection']['min_pause_duration']
    max_pause_duration = settings['gps']['parameters']['trip_detection']['max_pause_duration']

    ### mode of transportation
    detect_trip_mode = settings['gps']['parameters']['mode_of_transportation']['detect_trip_mode']
    vehicle_cutoff = settings['gps']['parameters']['mode_of_transportation']['vehicle_cutoff']
    bicycle_cutoff = settings['gps']['parameters']['mode_of_transportation']['bicycle_cutoff']
    walk_cutoff = settings['gps']['parameters']['mode_of_transportation']['walk_cutoff']
    percentile_to_sample = settings['gps']['parameters']['mode_of_transportation']['percentile_to_sample']
    min_segment_length = settings['gps']['parameters']['mode_of_transportation']['min_segment_length']

    ## Accelerometer parameters
    ###########################

    ### general
    include_acc = settings['accelerometer']['parameters']['include_acc']
    include_vect = settings['accelerometer']['parameters']['include_vect']
    mark_not_wearing_time = settings['accelerometer']['parameters']['not_wearing_time']['mark_not_wearing_time']
    minutes_zeros_row = settings['accelerometer']['parameters']['not_wearing_time']['minutes_zeros_row']

    ### activity bouts
    detect_activity_bouts = settings['accelerometer']['parameters']['activity_bout']['detect_activity_bouts']
    activity_bout_duration = settings['accelerometer']['parameters']['activity_bout']['activity_bout_duration']
    activity_bout_upper_limit = settings['accelerometer']['parameters']['activity_bout']['activity_bout_upper_limit']
    activity_bout_lower_limit = settings['accelerometer']['parameters']['activity_bout']['activity_bout_lower_limit']
    activity_bout_tolerance = settings['accelerometer']['parameters']['activity_bout']['activity_bout_tolerance']

    ### sedentary bouts
    detect_sedentary_bouts = settings['accelerometer']['parameters']['sedentary_bout']['detect_sedentary_bouts']
    sedentary_bout_duration = settings['accelerometer']['parameters']['sedentary_bout']['sedentary_bout_duration']
    sedentary_bout_upper_limit = settings['accelerometer']['parameters']['sedentary_bout']['sedentary_bout_upper_limit']
    sedentary_bout_tolerance = settings['accelerometer']['parameters']['sedentary_bout']['sedentary_bout_tolerance']

    ### classification settings
    very_hard_cutoff = settings['accelerometer']['parameters']['activity_classification']['very_hard_cutoff']
    hard_cutoff = settings['accelerometer']['parameters']['activity_classification']['hard_cutoff']
    moderate_cutoff = settings['accelerometer']['parameters']['activity_classification']['moderate_cutoff']
    light_cutoff = settings['accelerometer']['parameters']['activity_classification']['light_cutoff']

    ## Merge options
    merge_data_to_gps = settings['merge_options']['merge_data_to_gps']
    merge_data_to_acc = settings['merge_options']['merge_data_to_acc']

    print(" ")

    ts_name = 'timestamp'
    speed_col = 'speed'
    ele_col = 'height'
    dist_col = 'distance'
    fix_type_col = 'fixTypeCode'
    AC_name = 'activity'
    AI_name = 'activityIntensity'

    id = 1
    for file_acc, file_gps in list_combined:

        print("Dataset: {}".format(file_acc))
        print(" ")

        ## process accelerometer data
        print("processing accelerometer data\n")

        ## read raw accelerometer data
        acc_data_raw = spark.read.text(acc_path + file_acc)

        step, acc_data = gen_acc_dataframe(acc_data_raw, ts_name)

        acc_columns = ['axis1', 'axis2', 'axis3', 'steps', 'lux', 'incl_off', 'incl_standing', 'incl_sitting',
                       'incl_lying']

        acc_data = split_acc_data(acc_data, acc_columns)
        acc_data = select_acc_intervals(acc_data, ts_name, step, interval, include_vect, include_acc).cache()
        acc_data.count()

        print(" ===> determine activity count...")
        start_time = time.time()
        acc_data = activity_count(acc_data, ts_name, interval,
                                  light_cutoff, moderate_cutoff, hard_cutoff, very_hard_cutoff, include_acc).cache()
        acc_data.count()
        elapsed_time = time.time() - start_time
        print("      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
        print(" ")

        if mark_not_wearing_time:
            print(" ===> determine non-wearing period...")
            start_time = time.time()
            acc_data = non_wear_filter(acc_data, ts_name, AC_name, AI_name, interval, minutes_zeros_row).cache()
            acc_data.count()
            elapsed_time = time.time() - start_time
            print("      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
            print(" ")

        if detect_activity_bouts:
            print(" ===> detect activity bouts...")
            acc_data = activity_bout_filter(acc_data, ts_name, AC_name, 'activityBoutNumber', interval,
                                            activity_bout_upper_limit, activity_bout_lower_limit,
                                            activity_bout_duration, activity_bout_tolerance).cache()
            acc_data.count()
            print("      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
            print(" ")

        if detect_sedentary_bouts:
            print(" ===> detect sedentary bouts...")
            acc_data = sedentary_bout_filter(acc_data, ts_name, AC_name, 'sedentaryBoutNumber', interval,
                                             sedentary_bout_upper_limit, 0,
                                             sedentary_bout_duration, sedentary_bout_tolerance).cache()
            acc_data.count()
            print("      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
            print(" ")

        acc_data = acc_data.withColumn('ID', F.lit(id)).orderBy(ts_name)
        cols = acc_data.columns
        cols.remove('ID')
        cols.insert(0, 'ID')
        acc_data = acc_data.select(cols).orderBy(ts_name).cache()
        acc_data = acc_data.checkpoint()
        acc_data.count()

        print("Dataset: {}".format(file_gps))
        print(" ")

        ## Process GPS data
        print("processing GPS data\n")

        ## Read raw GPS data
        gps_data_raw = spark.read.csv(gps_path + file_gps, header=True, inferSchema=True).cache()
        gps_data_raw.count()

        date_format = 'yyyy/MM/dd'
        time_format = 'HH:mm:ss'
        datetime_format = date_format + ' ' + time_format

        gps_data = gen_gps_dataframe(gps_data_raw, ts_name, datetime_format).cache()
        num_fixes = gps_data.count()

        ## Filter data according to new epoch
        print(" ===> select GPS data every {} seconds...".format(str(interval)))

        if interval == step:

            print("      keep data for each epoch")
            print(" ")

        elif interval < step:

            print("      interval smaller than one epoch ({} seconds): keep all GPS data".format(str(step)))
            print(" ")

        else:

            start_time = time.time()
            gps_data = select_gps_intervals(gps_data, ts_name, interval).cache()
            num_fixes = gps_data.count()
            elapsed_time = time.time() - start_time
            print("      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
            print(" ")

        print(" ===> set fix type...")
        start_time = time.time()
        gps_data = set_fix_type(gps_data, ts_name, fix_type_col, los_max_duration).cache()
        elapsed_time = time.time() - start_time
        print("      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
        print("      number of fixes: {}".format(str(num_fixes)))
        print(" ")

        ## Calculate distance and speed at each epoch
        gps_data = set_distance_and_speed(gps_data, dist_col, speed_col, ts_name, fix_type_col).cache()
        gps_data.count()

        ## Apply filters
        if filter_invalid_values:
            print(" ===> apply velocity filter...")
            start_time = time.time()
            gps_data = filter_speed(gps_data, speed_col, fix_type_col, max_speed).cache()
            diff_fixes = num_fixes - gps_data.count()
            num_fixes = num_fixes - diff_fixes
            elapsed_time = time.time() - start_time
            print("      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
            print("      {} fixes filtered out".format(str(diff_fixes)))
            print(" ")

            print(" ===> apply acceleration filter...")
            start_time = time.time()
            gps_data = filter_acceleration(gps_data, speed_col, ts_name, fix_type_col).cache()
            diff_fixes = num_fixes - gps_data.count()
            num_fixes = num_fixes - diff_fixes
            elapsed_time = time.time() - start_time
            print("      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
            print("      {} fixes filtered out".format(str(diff_fixes)))
            print(" ")

            print(" ===> apply elevation change filter...")
            start_time = time.time()
            gps_data = filter_height(gps_data, ele_col, ts_name, fix_type_col, max_ele_change).cache()
            diff_fixes = num_fixes - gps_data.count()
            num_fixes = num_fixes - diff_fixes
            elapsed_time = time.time() - start_time
            print("      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
            print("      {} fixes filtered out".format(str(diff_fixes)))
            print(" ")

            print(" ===> apply three fixes filter...")
            start_time = time.time()
            gps_data = filter_change_dist_3_fixes(gps_data, dist_col, ts_name, fix_type_col, min_change_3_fixes).cache()
            diff_fixes = num_fixes - gps_data.count()
            num_fixes = num_fixes - diff_fixes
            elapsed_time = time.time() - start_time
            print("      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
            print("      {} fixes filtered out".format(str(diff_fixes)))
            print(" ")

        print(" ===> align timestamps...")
        start_time = time.time()
        gps_data = round_timestamp(gps_data, ts_name, interval).cache()
        gps_data = gps_data.checkpoint()
        diff_fixes = num_fixes - gps_data.count()
        num_fixes = num_fixes - diff_fixes
        elapsed_time = time.time() - start_time
        print("      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
        print("      number of fixes after all filters applied: {}".format(str(num_fixes)))
        print(" ")

        ## Use a subset of GPS data
        # gps_data = gps_data.limit(20000)

        ## Trip detection
        if trip_detection:

            print(" ===> detect trips...")
            start_time = time.time()
            ## set 4 partitions (use this value only in local mode)
            gps_data = detect_trips(gps_data, ts_name, dist_col, speed_col, fix_type_col, min_distance,
                                    min_pause_duration, max_pause_duration, max_speed).cache()
            gps_data = gps_data.checkpoint()
            gps_data.count()
            elapsed_time = time.time() - start_time
            print("      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
            print(" ")

            if detect_trip_mode:
                print(" ===> classify trips...")
                start_time = time.time()
                gps_data = classify_trips(gps_data, ts_name, dist_col, speed_col, fix_type_col,
                                          vehicle_cutoff, bicycle_cutoff, walk_cutoff,
                                          min_trip_length, min_trip_duration, min_segment_length,
                                          percentile_to_sample).cache()
                gps_data = gps_data.checkpoint()
                gps_data.count()
                elapsed_time = time.time() - start_time
                print("      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
                print(" ")

        ## Insert missing data
        if insert_missing:

            if insert_until:

                if insert_max_seconds > los_max_duration:
                    insert_max_seconds = los_max_duration
            else:
                insert_max_seconds = los_max_duration

            print(" ===> fill in missing values...")
            start_time = time.time()
            gps_data = fill_timestamp(gps_data, ts_name, fix_type_col, interval, insert_max_seconds).cache()
            gps_data = gps_data.checkpoint()
            diff_fixes = gps_data.count() - num_fixes
            num_fixes = num_fixes + diff_fixes
            elapsed_time = time.time() - start_time
            print("      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))
            print("      number of fixes after inserts: {}".format(str(num_fixes)))
            print(" ")

        gps_data = gps_data.withColumn('ID', F.lit(id)).orderBy(ts_name)
        cols = gps_data.columns
        cols.remove('ID')
        cols.insert(0, 'ID')
        gps_data = gps_data.select(cols).orderBy(ts_name).cache()
        gps_data = gps_data.checkpoint()
        gps_data.count()

        ## Merge dataframes
        if merge_data_to_gps:
            print("merging accelerometer data to GPS data\n")

            merged_data = gps_data.join(acc_data, ['ID', ts_name], how='left').orderBy(ts_name)

            merged_data = merged_data.cache()
            merged_data = merged_data.checkpoint()
            merged_data.count()

            ## save combined dataframe
            merged_data.toPandas().to_csv('PALMSpy_output/' + output_filename + "_gps_acc_{}.csv".format(str(id)),
                                          index=False)
            print(" ===> merged dataframe saved in: " + output_filename + "_gps_acc_{}.csv".format(str(id)))
            print(" ")

        if merge_data_to_acc:

            print("merging GPS data to accelerometer data\n")
            merged_data = acc_data.join(gps_data, ['ID', ts_name], how='left').orderBy(ts_name)

            merged_data = merged_data.cache()
            merged_data = merged_data.checkpoint()
            merged_data.count()

            merged_data_pd = merged_data.toPandas()

            merged_data_pd.fixTypeCode = merged_data_pd.fixTypeCode.astype('Int64')

            if trip_detection:
                merged_data_pd.tripNumber = merged_data_pd.tripNumber.astype('Int64')
                merged_data_pd.tripType = merged_data_pd.tripType.astype('Int64')

            ## save combined dataframe
            merged_data_pd.to_csv('PALMSpy_output/' + output_filename + "_acc_gps_{}.csv".format(str(id)),
                                  index=False)

            print(" ===> merged dataframe saved in: " + output_filename + "_acc_gps_{}.csv".format(str(id)))
            print(" ")

        if not merge_data_to_acc and not merge_data_to_gps:
            ## save processed GPS data
            gps_data.toPandas().to_csv('PALMSpy_output/' + file_gps[:-4] + '_palmspy_gps.csv', index=False)
            print(" ===> GPS data saved in: {}_palmspy_gps.csv".format(file_gps[:-4]))

            ## save processed accelerometer data
            acc_data.toPandas().to_csv('PALMSpy_output/' + file_acc[:-4] + '_palmspy_acc.csv', index=False)
            print(" ===> accelerometer data saved in: {}_palmspy_acc.csv".format(file_acc[:-4]))
            print(" ")

        print(" ")

        ## Remove all checkpoints
        try:
            shutil.rmtree('checkpoints')
        except:
            pass

        id += 1

    ## Merge output files in one single file
    if (merge_data_to_gps):
        list_procs = sorted(glob.glob("PALMSpy_output/*gps_acc*.csv"), key=os.path.getmtime)

        header_saved = False
        if len(list_procs) > 1:
            with open('PALMSpy_output/' + output_filename + '_gps_acc_all.csv', 'w') as fout:
                for filename in list_procs:
                    with open(filename) as fin:
                        head = next(fin)
                        if not header_saved:
                            fout.write(head)
                            header_saved = True
                        for line in fin:
                            fout.write(line)

    if (merge_data_to_acc):
        list_procs = sorted(glob.glob("PALMSpy_output/*acc_gps*.csv"), key=os.path.getmtime)

        header_saved = False
        if len(list_procs) > 1:
            with open('PALMSpy_output/' + output_filename + '_acc_gps_all.csv', 'w') as fout:
                for filename in list_procs:
                    with open(filename) as fin:
                        head = next(fin)
                        if not header_saved:
                            fout.write(head)
                            header_saved = True
                        for line in fin:
                            fout.write(line)

    program_duration = time.time() - program_start
    print("Program completed in: {}".format(time.strftime("%H:%M:%S", time.gmtime(program_duration))))
    print(" ")


if __name__ == "__main__":
    arguments = parser.parse_args()
    main(**vars(arguments))
