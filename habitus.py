#!/usr/bin/env python
# coding: utf-8

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import textwrap
import json
from tools import PColors as pc
from tools import Input, Metadata
import os, sys
import shutil
import glob

from help_menu import parser
from GPSProcessing import *
from AccProcessing import *

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
        print(pc.BOLD + element + pc.ENDC)
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
         merge_data_to_gps,
         driver_mem, executor_mem, mem_fraction, shuffle_partitions, mem_offHeap_enabled,
         mem_offHeap_size, clean_checkpoints, codegen_wholeStage, codegen_fallback,
         broadcast_timeout, network_timeout):
         #json_filename):

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

    # Output directory
    if not os.path.exists('HABITUS_output'):
        os.makedirs('HABITUS_output')

    # Load default configuration parameters and update them from command line
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
         merge_data_to_gps,
         driver_mem, executor_mem, mem_fraction, shuffle_partitions, mem_offHeap_enabled,
         mem_offHeap_size, clean_checkpoints, codegen_wholeStage, codegen_fallback,
         broadcast_timeout, network_timeout)
    settings = params.dump_dict()

    #  Overwrite configuration parameters from file or save them to file
    if config_file == "":
        with open('settings.json', 'w') as f:
            json.dump(settings, f)
        config_file = 'settings.json'
    else:
        file_settings = open(config_file, "r")
        with file_settings as f:
            settings = json.load(f)

    # Set Spark configuration parameters
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
                               ('spark.executor.extraJavaOptions', '-XX:ReservedCodeCacheSize=384m -XX:+UseCodeCacheFlushing')
                               ]
                              )
    # ('spark.driver.host', 'localhost') # TODO: allows to pass configs with spark-submit, without spark.conf file
    # ('spark.ui.port', '3000')

    spark = SparkSession.builder.config(conf=conf).master("local[*]").appName("HABITUS").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir('checkpoints')
    sc.getConf().getAll()

    spark.catalog.clearCache()

    header()

    print("Number of CPU cores: {}".format(str(sc.defaultParallelism)))

    # List accelerometer and GPS data files have the same name
    list_file_acc = sorted(os.listdir(acc_path))
    list_file_gps = sorted(os.listdir(gps_path))
    list_combined = list(zip(list_file_acc, list_file_gps))

    output_filename = 'partial_'

    # GPS parameters
    ################
    # TODO: add option to remove lone fixes

    ## general
    interval = settings['gps']['parameters']['general']['interval']
    insert_missing = settings['gps']['parameters']['general']['insert_missing']
    insert_until = settings['gps']['parameters']['general']['insert_until']
    insert_max_seconds = settings['gps']['parameters']['general']['insert_max_seconds']
    los_max_duration = settings['gps']['parameters']['general']['los_max_duration']

    ## filtering
    filter_invalid_values = settings['gps']['parameters']['filter_options']['filter_invalid_values']
    remove_lone_fixes = settings['gps']['parameters']['filter_options']['remove_lone_fixes']
    max_speed = settings['gps']['parameters']['filter_options']['max_speed']
    max_ele_change = settings['gps']['parameters']['filter_options']['max_ele_change']
    min_change_3_fixes = settings['gps']['parameters']['filter_options']['min_change_3_fixes']

    ## trip detection
    trip_detection = settings['gps']['parameters']['trip_detection']['detect_trip']
    min_distance = settings['gps']['parameters']['trip_detection']['min_distance']
    min_trip_length = settings['gps']['parameters']['trip_detection']['min_trip_length']
    min_trip_duration = settings['gps']['parameters']['trip_detection']['min_trip_duration']
    min_pause_duration = settings['gps']['parameters']['trip_detection']['min_pause_duration']
    max_pause_duration = settings['gps']['parameters']['trip_detection']['max_pause_duration']

    ## mode of transportation
    detect_trip_mode = settings['gps']['parameters']['mode_of_transportation']['detect_trip_mode']
    vehicle_cutoff = settings['gps']['parameters']['mode_of_transportation']['vehicle_cutoff']
    bicycle_cutoff = settings['gps']['parameters']['mode_of_transportation']['bicycle_cutoff']
    walk_cutoff = settings['gps']['parameters']['mode_of_transportation']['walk_cutoff']
    percentile_to_sample = settings['gps']['parameters']['mode_of_transportation']['percentile_to_sample']
    min_segment_length = settings['gps']['parameters']['mode_of_transportation']['min_segment_length']

    # Accelerometer parameters
    ##########################

    ## general
    include_acc = settings['accelerometer']['parameters']['include_acc']
    include_vect = settings['accelerometer']['parameters']['include_vect']
    mark_not_wearing_time = settings['accelerometer']['parameters']['not_wearing_time']['mark_not_wearing_time']
    minutes_zeros_row = settings['accelerometer']['parameters']['not_wearing_time']['minutes_zeros_row']

    ## activity bouts
    detect_activity_bouts = settings['accelerometer']['parameters']['activity_bout']['detect_activity_bouts']
    activity_bout_duration = settings['accelerometer']['parameters']['activity_bout']['activity_bout_duration']
    activity_bout_upper_limit = settings['accelerometer']['parameters']['activity_bout']['activity_bout_upper_limit']
    activity_bout_lower_limit = settings['accelerometer']['parameters']['activity_bout']['activity_bout_lower_limit']
    activity_bout_tolerance = settings['accelerometer']['parameters']['activity_bout']['activity_bout_tolerance']

    ## sedentary bouts
    detect_sedentary_bouts = settings['accelerometer']['parameters']['sedentary_bout']['detect_sedentary_bouts']
    sedentary_bout_duration = settings['accelerometer']['parameters']['sedentary_bout']['sedentary_bout_duration']
    sedentary_bout_upper_limit = settings['accelerometer']['parameters']['sedentary_bout']['sedentary_bout_upper_limit']
    sedentary_bout_tolerance = settings['accelerometer']['parameters']['sedentary_bout']['sedentary_bout_tolerance']

    ## classification settings
    very_hard_cutoff = settings['accelerometer']['parameters']['activity_classification']['very_hard_cutoff']
    hard_cutoff = settings['accelerometer']['parameters']['activity_classification']['hard_cutoff']
    moderate_cutoff = settings['accelerometer']['parameters']['activity_classification']['moderate_cutoff']
    light_cutoff = settings['accelerometer']['parameters']['activity_classification']['light_cutoff']

    # Merge options
    merge_data_to_gps = settings['merge_options']['merge_data_to_gps']

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

        print(pc.OKGREEN  + "Dataset: {}".format(file_acc) + pc.ENDC)
        print(" ")

        # Process accelerometer data
        print(pc.OKBLUE + pc.UNDERLINE + "Process Accelerometer Data\n" + pc.ENDC)

        # Read raw accelerometer data
        acc_data_raw = spark.read.text(acc_path + file_acc)
        acc_data_raw.checkpoint()

        step, acc_data = gen_acc_dataframe(acc_data_raw)

        acc_columns = ['axis1', 'axis2', 'axis3', 'steps', 'lux', 'incl_off', 'incl_standing', 'incl_sitting',
                       'incl_lying']

        acc_data = split_acc_data(acc_data, acc_columns)
        acc_data = select_acc_intervals(acc_data, ts_name, step, interval, include_vect, include_acc).cache()
        acc_data = acc_data.checkpoint()
        acc_data.count()

        print(pc.WARNING + " ===> determine activity count..." + pc.ENDC)
        start_time = time.time()
        acc_data = activity_count(acc_data, ts_name, interval,
                                  light_cutoff, moderate_cutoff, hard_cutoff, very_hard_cutoff, include_acc).cache()
        acc_data = acc_data.checkpoint()
        acc_data.count()
        elapsed_time = time.time() - start_time
        print(pc.WARNING + "      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))) + pc.ENDC)
        print(" ")

        if mark_not_wearing_time:
            print(pc.WARNING + " ===> determine non-wearing period..." + pc.ENDC)
            start_time = time.time()
            acc_data = non_wear_filter(acc_data, ts_name, AC_name, AI_name, interval, minutes_zeros_row).cache()
            acc_data = acc_data.checkpoint()
            acc_data.count()
            elapsed_time = time.time() - start_time
            print(pc.WARNING + "      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))) + pc.ENDC)
            print(" ")

        if detect_activity_bouts:
            print(pc.WARNING + " ===> detect activity bouts..." + pc.ENDC)
            acc_data = activity_bout_filter(acc_data, ts_name, AC_name, 'activityBoutNumber', interval,
                                            activity_bout_upper_limit, activity_bout_lower_limit,
                                            activity_bout_duration, activity_bout_tolerance).cache()
            acc_data = acc_data.checkpoint()
            acc_data.count()
            print(pc.WARNING + "      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))) + pc.ENDC)
            print(" ")

        if detect_sedentary_bouts:
            print(pc.WARNING + " ===> detect sedentary bouts..." + pc.ENDC)
            acc_data = sedentary_bout_filter(acc_data, ts_name, AC_name, 'sedentaryBoutNumber', interval,
                                            sedentary_bout_upper_limit, 0,
                                            sedentary_bout_duration, sedentary_bout_tolerance).cache()
            acc_data = acc_data.checkpoint()
            acc_data.count()
            print(pc.WARNING + "      time elapsed: {}".format(
                time.strftime("%H:%M:%S", time.gmtime(elapsed_time))) + pc.ENDC)
            print(" ")

        print(pc.OKGREEN + "Dataset: {}".format(file_gps) + pc.ENDC)
        print(" ")

        # Process GPS data
        print(pc.OKBLUE + pc.UNDERLINE + "Process GPS Data\n" + pc.ENDC)

        # Read raw GPS data
        gps_data_raw = spark.read.csv(gps_path + file_gps, header=True, inferSchema=True).cache()
        gps_data_raw = gps_data_raw.checkpoint()
        gps_data_raw.count()

        date_format = 'yyyy/MM/dd'
        time_format = 'HH:mm:ss'
        datetime_format = date_format + ' ' + time_format

        gps_data = gen_gps_dataframe(gps_data_raw, datetime_format, id).cache()
        gps_data = gps_data.checkpoint()
        gps_data.count()

        print(pc.WARNING + " ===> set fix type..." + pc.ENDC)
        start_time = time.time()
        gps_data = set_fix_type(gps_data, ts_name, los_max_duration).cache()
        gps_data = gps_data.checkpoint()
        gps_data.count()
        elapsed_time = time.time() - start_time
        print(pc.WARNING + "      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))) + pc.ENDC)
        print(" ")

        # Apply filters
        if filter_invalid_values:

            print(pc.WARNING + " ===> apply velocity filter..." + pc.ENDC)
            start_time = time.time()
            gps_data = filter_speed(gps_data, speed_col, max_speed).cache()
            gps_data = gps_data.checkpoint()
            gps_data.count()
            elapsed_time = time.time() - start_time
            print(pc.WARNING + "      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))) + pc.ENDC)
            print(" ")

            print(pc.WARNING + " ===> apply accelaration filter..." + pc.ENDC)
            start_time = time.time()
            gps_data = filter_acceleration(gps_data, speed_col, ts_name).cache()
            gps_data = gps_data.checkpoint()
            gps_data.count()
            elapsed_time = time.time() - start_time
            print(pc.WARNING + "      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))) + pc.ENDC)
            print(" ")

            print(pc.WARNING + " ===> apply elevation change filter..." + pc.ENDC)
            start_time = time.time()
            gps_data = filter_height(gps_data, ele_col, ts_name, max_ele_change).cache()
            gps_data = gps_data.checkpoint()
            gps_data.count()
            elapsed_time = time.time() - start_time
            print(pc.WARNING + "      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))) + pc.ENDC)
            print(" ")

            print(pc.WARNING + " ===> apply three fixes filter..." + pc.ENDC)
            start_time = time.time()
            gps_data = filter_change_dist_3_fixes(gps_data, dist_col, ts_name, min_change_3_fixes).cache()
            gps_data = gps_data.checkpoint()
            gps_data.count()
            elapsed_time = time.time() - start_time
            print(pc.WARNING + "      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))) + pc.ENDC)
            print(" ")

        # Filter data according to new epoch
        print(pc.WARNING +  " ===> select GPS data every {} seconds...".format(str(interval)) + pc.ENDC)

        if interval == step:
            print(pc.WARNING + "      keep data for each epoch" + pc.ENDC)
            print(" ")
        elif interval < step:
            print(pc.FAIL + "      interval smaller than one epoch ({} seconds): keep all GPS data".format(str(step)) + pc.ENDC)
            print(" ")
        else:
            start_time = time.time()
            gps_data = select_gps_intervals(gps_data, ts_name, interval).cache()
            gps_data = gps_data.checkpoint()
            gps_data.count()
            elapsed_time = time.time() - start_time
            print(pc.WARNING + "      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))) + pc.ENDC)
            print(" ")

        print(pc.WARNING + " ===> align timestamps..." + pc.ENDC)
        start_time = time.time()
        gps_data = round_timestamp(gps_data, ts_name, interval).cache()
        gps_data = gps_data.checkpoint()
        gps_data.count()
        elapsed_time = time.time() - start_time
        print(pc.WARNING + "      time elapsed: {}".format(
            time.strftime("%H:%M:%S", time.gmtime(elapsed_time))) + pc.ENDC)
        print(" ")

        #gps_data = gps_data.limit(100) ######################################<<<<<<<<<<<<<<<

        # Trip detection
        if trip_detection:

            print(pc.WARNING + " ===> detect trips..." + pc.ENDC)
            start_time = time.time()
            gps_data = detect_trips(gps_data, ts_name, dist_col, speed_col, fix_type_col, min_distance,
                                    min_pause_duration, max_pause_duration, max_speed).cache()
            gps_data = gps_data.checkpoint()
            gps_data.count()
            elapsed_time = time.time() - start_time
            print(pc.WARNING + "      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))) + pc.ENDC)
            print(" ")

            if detect_trip_mode:

                print(pc.WARNING + " ===> classify trips..." + pc.ENDC)
                start_time = time.time()
                gps_data = classify_trips(gps_data, ts_name, dist_col, speed_col,
                                         vehicle_cutoff, bicycle_cutoff, walk_cutoff,
                                         min_trip_length, min_trip_duration, min_segment_length,
                                         percentile_to_sample).cache()
                gps_data = gps_data.checkpoint()
                gps_data.count()
                elapsed_time = time.time() - start_time
                print(pc.WARNING + "      time elapsed: {}".format(time.strftime("%H:%M:%S", time.gmtime(elapsed_time))) + pc.ENDC)
                print(" ")

        # Insert missing data
        if insert_missing:

            if insert_until:
                if insert_max_seconds > los_max_duration:
                    insert_max_seconds = los_max_duration
            else:
                insert_max_seconds = los_max_duration

            print(pc.WARNING + " ===> fill in missing value..." + pc.ENDC)
            start_time = time.time()
            gps_data = fill_timestamp(gps_data, ts_name, fix_type_col, step, insert_max_seconds).cache()
            gps_data = gps_data.checkpoint()
            gps_data.count()
            elapsed_time = time.time() - start_time
            print(pc.WARNING + "      time elapsed: {}".format(
                time.strftime("%H:%M:%S", time.gmtime(elapsed_time))) + pc.ENDC)
            print(" ")

        # Merge dataframes
        if merge_data_to_gps:
            print(pc.OKBLUE + pc.UNDERLINE + "Merge GPS and Accelerometer Data\n" + pc.ENDC)

            merged_data = gps_data.join(acc_data, [ts_name], how='left').orderBy(ts_name)

            ## change order of the columns
            cols = merged_data.columns
            cols.remove('ID')
            cols.insert(0, 'ID')

            merged_data = merged_data.select(cols).orderBy(ts_name).cache()

            merged_data = merged_data.checkpoint()
            merged_data.count()

            # Save combined dataframe
            merged_data.toPandas().to_csv('HABITUS_output/' + output_filename + "{}.csv".format(str(id)), index=False)
            print(pc.WARNING + " ===> merged dataframe saved as " + output_filename + "{}.csv".format(str(id)) + pc.ENDC)
            print("\n")
        else:
            # Save processed GPS data
            gps_data.toPandas().to_csv('HABITUS_output/' + file_gps + '-gps.csv', index=False)
            print(pc.WARNING + " ===> GPS processed data saved as {}-gps.csv".format(file_gps) + pc.ENDC)

            # Save processed accelerometer data
            acc_data.toPandas().to_csv('HABITUS_output/' + file_acc + '-acc.csv', index=False)
            print(pc.WARNING + " ===> accelerometer processed data saved as {}-acc.csv".format(file_acc) + pc.ENDC)
            print("\n")

        # Remove all checkpoints
        shutil.rmtree('checkpoints')

        id += 1

    # Merge output files in one single file
    if merge_data_to_gps:
        list_procs = sorted(glob.glob("HABITUS_output/*.csv"))

        header_saved = False
        with open('HABITUS_output/HABITUS_output_all.csv', 'w') as fout:
            for filename in list_procs:
                with open(filename) as fin:
                    head = next(fin)
                    if not header_saved:
                        fout.write(head)
                        header_saved = True
                    for line in fin:
                        fout.write(line)

    # Copy JSON config file into output folder
    shutil.copy(config_file, 'HABITUS_output/')

if __name__ == "__main__":
    arguments = parser.parse_args()
    main(**vars(arguments))
