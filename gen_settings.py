#!/usr/bin/env python

from tools import Input
import argparse


def main(interval=30, insert_missing=True, insert_until=False,
         insert_max_seconds=600, loss_max_duration=60, filter_invalid_values=True,
         max_speed = 130, max_ele_change=1000, min_change_3_fixes=10,
         detect_trip=True, min_distance=34, min_trip_length=100, min_trip_duration=180,
		 min_pause_duration=180, max_pause_duration=300, detect_trip_mode=True,
		 vehicle_cutoff=25, bicycle_cutoff=10, walk_cutoff=1,
		 percentile_to_sample=90, min_segment_length=30,
         include_acc=False, include_vect=False,
		 mark_not_wearing_time=True, minutes_zeros_row=30,
		 detect_activity_bouts=True, activity_bout_duration=5,
		 activity_bout_upper_limit=9999, activity_bout_lower_limit=1953, activity_bout_tolerance=2,
		 detect_sedentary_bouts=True, sedentary_bout_duration=30,
		 sedentary_bout_upper_limit=100, sedentary_bout_tolerance=1,
		 very_hard_cutoff=9498, hard_cutoff=5725, moderate_cutoff=1953, light_cutoff=100,
		 merge_data_to_gps=True,
         driver_mem='16g', executor_mem='16g', mem_fraction=0.6, shuffle_partitions=20, mem_offHeap_enabled=True,
         mem_offHeap_size='16g', clean_checkpoints=True, codegen_wholeStage=False, codegen_fallback=True,
         broadcast_timeout=1200, network_timeout='800s'):


	#TODO: add option to remove lone fixes
	params = Input()
	settings = params.dump_dict()

	# GPS parameters
	settings['gps']['parameters']['general']['interval'] = interval
	settings['gps']['parameters']['general']['insert_missing'] = insert_missing
	settings['gps']['parameters']['general']['insert_until'] = insert_until
	settings['gps']['parameters']['general']['insert_max_seconds'] = insert_max_seconds
	settings['gps']['parameters']['general']['los_max_duration'] = loss_max_duration
	settings['gps']['parameters']['filter_options']['filter_invalid_values'] = filter_invalid_values
	settings['gps']['parameters']['filter_options']['max_speed'] = max_speed
	settings['gps']['parameters']['filter_options']['max_ele_change'] = max_ele_change
	settings['gps']['parameters']['filter_options']['min_change_3_fixes'] = min_change_3_fixes
	settings['gps']['parameters']['trip_detection']['detect_trip'] = detect_trip
	settings['gps']['parameters']['trip_detection']['min_distance'] = min_distance
	settings['gps']['parameters']['trip_detection']['min_trip_length'] = min_trip_length
	settings['gps']['parameters']['trip_detection']['min_trip_duration'] = min_trip_duration
	settings['gps']['parameters']['trip_detection']['min_pause_duration'] = min_pause_duration
	settings['gps']['parameters']['trip_detection']['max_pause_duration'] = max_pause_duration
	settings['gps']['parameters']['mode_of_transportation']['detect_trip_mode'] = detect_trip_mode
	settings['gps']['parameters']['mode_of_transportation']['vehicle_cutoff'] = vehicle_cutoff
	settings['gps']['parameters']['mode_of_transportation']['bicycle_cutoff'] = bicycle_cutoff
	settings['gps']['parameters']['mode_of_transportation']['walk_cutoff'] = walk_cutoff
	settings['gps']['parameters']['mode_of_transportation']['percentile_to_sample'] = percentile_to_sample
	settings['gps']['parameters']['mode_of_transportation']['min_segment_length'] = min_segment_length

	# Accelerometer parameters
	settings['accelerometer']['parameters']['include_acc'] = include_acc
	settings['accelerometer']['parameters']['include_vect'] = include_vect
	settings['accelerometer']['parameters']['not_wearing_time']['mark_not_wearing_time'] = mark_not_wearing_time
	settings['accelerometer']['parameters']['not_wearing_time']['minutes_zeros_row'] = minutes_zeros_row
	settings['accelerometer']['parameters']['activity_bout']['detect_activity_bouts'] = detect_activity_bouts
	settings['accelerometer']['parameters']['activity_bout']['activity_bout_duration'] = activity_bout_duration
	settings['accelerometer']['parameters']['activity_bout']['activity_bout_upper_limit'] = activity_bout_upper_limit
	settings['accelerometer']['parameters']['activity_bout']['activity_bout_lower_limit'] = activity_bout_lower_limit
	settings['accelerometer']['parameters']['activity_bout']['activity_bout_tolerance'] = activity_bout_tolerance
	settings['accelerometer']['parameters']['sedentary_bout']['detect_sedentary_bouts'] = detect_sedentary_bouts
	settings['accelerometer']['parameters']['sedentary_bout']['sedentary_bout_duration'] = sedentary_bout_duration
	settings['accelerometer']['parameters']['sedentary_bout']['sedentary_bout_upper_limit'] = sedentary_bout_upper_limit
	settings['accelerometer']['parameters']['sedentary_bout']['sedentary_bout_tolerance'] = sedentary_bout_tolerance
	settings['accelerometer']['parameters']['activity_classification']['very_hard_cutoff'] = very_hard_cutoff
	settings['accelerometer']['parameters']['activity_classification']['hard_cutoff'] = hard_cutoff
	settings['accelerometer']['parameters']['activity_classification']['moderate_cutoff'] = moderate_cutoff
	settings['accelerometer']['parameters']['activity_classification']['light_cutoff'] = light_cutoff

	# Merge options
	settings['merge_options']['merge_data_to_gps'] = merge_data_to_gps

	# Spark parameters
	settings['spark']['memory']['fraction'] = mem_fraction
	settings['spark']['executor']['memory'] = executor_mem
	settings['spark']['driver']['memory'] = driver_mem
	settings['spark']['sql']['shuffle']['partitions'] = shuffle_partitions
	settings['spark']['memory']['offHeap']['enabled'] = mem_offHeap_enabled
	settings['spark']['memory']['offHeap']['size'] = mem_offHeap_size
	settings['spark']['cleaner']['referenceTracking']['cleanCheckpoints'] = clean_checkpoints
	settings['spark']['sql']['codegen']['wholeStage'] = codegen_wholeStage
	settings['spark']['sql']['codegen']['fallback'] = codegen_fallback
	settings['spark']['sql']['broadcastTimeout'] = broadcast_timeout
	settings['spark']['network']['timeout'] = network_timeout

	#print(settings)

# refer tot he program name: %(prog)s
parser = argparse.ArgumentParser(
	prog='gen_settings',
	usage='%(prog)s [options]',
	description="Import input parameters for Spark, GPS and Accelerometer configuration.\
                 Spark runs in 'local' mode. GPS/Accelerometer settings match the default PALMS configuration.",
	formatter_class=argparse.ArgumentDefaultsHelpFormatter
)

# GPS options
parser.add_argument(
	"--interval",
	type=int,
	dest="interval",
	default = 30,
	help="duration of interval between results in seconds"
)

parser.add_argument(
	"--insert-missing",
	type=bool,
	dest="insert_missing",
	default = True,
	help="if true, gaps in GPS fixes are replaced by the last valid fix"
)

parser.add_argument(
	"--insert-until",
	type=bool,
	dest="insert_until",
	default = False,
	help="if true, inserts until a max time (set by --insert-max-seconds) is reached.\
	      If false, inserts will be added until loss of signal time is reached"
)

parser.add_argument(
	"--insert-max-seconds",
	type=int,
	dest="insert_max_seconds",
	default = 600,
	help="max number of seconds to replace missing fixes with last valid fix (valid if --insert-until=true)"
)

parser.add_argument(
	"--loss-max-duration",
	type=int,
	dest="loss_max_duration",
	default = 60,
	help=" max number of minutes allowed to pass before loss of signal is declared"
)

parser.add_argument(
	"--filter-invalid-values",
	type=bool,
	dest="filter_invalid_values",
	default = True,
	help="if true, removes invalid fixes"
)

parser.add_argument(
	"--max-speed",
	type=int,
	dest="max_speed",
	default = 130,
	help="fix is invalid if speed is greater than this value (in km/hr)"
)

parser.add_argument(
	"--max-ele-change",
	type=int,
	dest="max_ele_change",
	default = 1000,
	help="fix is invalid if elevation change is greater than this value (in meters)"
)

parser.add_argument(
	"--min-change-3-fixes",
	type=int,
	dest="min_change_3_fixes",
	default = 10,
	help="fix is invalid if change in distance between fix 1 and 3 is less than this value (in meters)"
)

parser.add_argument(
	"--detect-trip",
	type=bool,
	dest="detect_trip",
	default = True,
	help="if true, calculates the fix trip type: STATIONARY (0), START POINT (1), MID POINT (2), PAUSE POINT (3),\
	      and END POINT (4)"
)

parser.add_argument(
	"--min-distance",
	type=int,
	dest="min_distance",
	default = 34,
	help="minimum distance (in meters) that must be travelled over one minute to indicate the start of a trip.\
	      Default value corresponds to a typical walking speed of 2 Km/hr"
)

parser.add_argument(
	"--min-trip-length",
	type=int,
	dest="min_trip_length",
	default = 100,
	help="trips less than this distance (in meters) are not considered trips"
)

parser.add_argument(
	"--min-trip-duration",
	type=int,
	dest="min_trip_duration",
	default = 180,
	help="trips less than this duration (in seconds) are not considered trips"
)

parser.add_argument(
	"--min-pause-duration",
	type=int,
	dest="min_pause_duration",
	default = 180,
	help="when the duration at a location exceeds this value (in seconds), the point is marked as PAUSE POINT"
)

parser.add_argument(
	"--max-pause-duration",
	type=int,
	dest="max_pause_duration",
	default = 300,
	help=" when the duration of a pause exceeds this value (in seconds), the point is marked as an END POINT"
)

parser.add_argument(
	"--detect-trip-mode",
	type=bool,
	dest="detect_trip_mode",
	default = True,
	help="if true, calculates the mode of transportation based on the speed: STATIONARY (0),\
	      PEDESTRIAN (1), BICYCLE (2), and VEHICLE (3)"
)

parser.add_argument(
	"--vehicle-cutoff",
	type=int,
	dest="vehicle_cutoff",
	default = 25,
	help="speeds greater than this value (in Km/hr) will be marked as VEHICLE"
)

parser.add_argument(
	"--bicycle-cutoff",
	type=int,
	dest="bicycle_cutoff",
	default = 10,
	help="speeds greater than this value (in Km/hr) will be marked as BICYCLE"
)

parser.add_argument(
	"--walk-cutoff",
	type=int,
	dest="walk_cutoff",
	default = 1,
	help="speeds greater than this value (in Km/hr) will be marked as PEDESTRIAN"
)

parser.add_argument(
	"--percentile-to-sample",
	type=int,
	dest="percentile_to_sample",
	default = 90,
	help="speed comparisons are made at this percentile"
)

parser.add_argument(
	"--min-segment-length",
	type=int,
	dest="min_segment_length",
	default = 30,
	help="minimum length (in meters) of segments used to classify mode of transportation"
)

# Accelerometer options
parser.add_argument(
	"--include-acc",
	type=bool,
	dest="include_acc",
	default = False,
	help="if true, all measured accelerometer data are attached to the final output"
)

parser.add_argument(
	"--include-vect",
	type=bool,
	dest="include_vect",
	default = False,
	help="if true, the activity intensity is calculated from the accelerometer vector magnitude"
)

parser.add_argument(
	"--mark-not-wearing",
	type=bool,
	dest="mark_not_wearing_time",
	default = True,
	help="if true, it will mark not-wearing time (set actvity count and activity intensity equal to -2)"
)

parser.add_argument(
	"--minutes-zeros-row",
	type=int,
	dest="minutes_zeros_row",
	default = 30,
	help="minimum not-wearing time, corresponding to consecutive zeros in the activity count"
)

parser.add_argument(
	"--detect-activity-bouts",
	type=bool,
	dest="detect_activity_bouts",
	default = True,
	help="if true, it will detect activity bouts"
)

parser.add_argument(
	"--activity-bout-duration",
	type=int,
	dest="activity_bout_duration",
	default = 5,
	help="minimum activity bout duration (in minutes)"
)

parser.add_argument(
	"--activity-bout-up",
	type=int,
	dest="activity_bout_upper_limit",
	default = 9999,
	help="activity bout upper limit"
)

parser.add_argument(
	"--activity-bout-low",
	type=int,
	dest="activity_bout_lower_limit",
	default = 1953,
	help="activity bout lower limit"
)

parser.add_argument(
	"--activity-bout-tol",
	type=int,
	dest="activity_bout_tolerance",
	default = 2,
	help="activity bout tolerance"
)

parser.add_argument(
	"--detect-sedentary-bouts",
	type=bool,
	dest="detect_sedentary_bouts",
	default = True,
	help="if true, it will detect sedentary bouts"
)

parser.add_argument(
	"--sedentary-bout-duration",
	type=int,
	dest="sedentary_bout_duration",
	default = 30,
	help="sedentary bout duration (in minutes)"
)

parser.add_argument(
	"--sedentary-bout-up",
	type=int,
	dest="sedentary_bout_upper_limit",
	default = 100,
	help="sedentary bout upper limit"
)

parser.add_argument(
	"--sedentary-bout-tol",
	type=int,
	dest="sedentary_bout_tolerance",
	default = 1,
	help="sedentary bout tolerance"
)

parser.add_argument(
	"--very-hard-cut",
	type=int,
	dest="very_hard_cutoff",
	default = 9498,
	help="very hard activity cutoff value"
)

parser.add_argument(
	"--hard-cut",
	type=int,
	dest="hard_cutoff",
	default = 9498,
	help="hard activity cutoff value"
)

parser.add_argument(
	"--moderate-cut",
	type=int,
	dest="moderate_cutoff",
	default = 1953,
	help="moderate activity cutoff value"
)

parser.add_argument(
	"--light-cut",
	type=int,
	dest="light_cutoff",
	default = 100,
	help="light activity cutoff value"
)

# Merge options
parser.add_argument(
	"--merge-acc-to-gps",
	type=bool,
	dest="merge_data_to_gps",
	default = True,
	help="if true, the accelerometer data will be merged to the GPS data and exported in one single file.\
	      If, false, the processed accelerometer and GPS data will be saved in two different files"
)

# Spark options
parser.add_argument(
	"--mem-fraction",
	type=float,
	dest="mem_fraction",
	default = 0.6,
	help="expresses the size of the execution and storage memory a fraction of the (JVM heap space - 300MB).\
          The rest of the space (40%%) is reserved for user data structures, internal metadata in Spark,\
          and safeguarding against OOM errors in the case of sparse and unusually large records"
)

parser.add_argument(
	"--executor-mem",
	type=str,
	dest="executor_mem",
	default = '16g',
	help="amount of memory to use per executor process, in the same format as JVM memory strings\
	      with a size unit suffix (\"k\", \"m\", \"g\" or \"t\")"
)

parser.add_argument(
	"--driver-mem",
	type=str,
	dest="driver_mem",
	default = '16g',
	help="amount of memory to use for the driver process,\
	      i.e. where SparkContext is initialized, in the same format as JVM memory strings\
	      with a size unit suffix (\"k\", \"m\", \"g\" or \"t\")"
)

parser.add_argument(
	"--shuffle-partitions",
	type=int,
	dest="shuffle_partitions",
	default = 20,
	help="configures the number of partitions to use when shuffling data for joins or aggregations"
)

parser.add_argument(
	"--mem-offHeap-enabled",
	type=bool,
	dest="mem_offHeap_enabled",
	default = True,
	help="if true, off-heap memory for ill be used for certain operations.\
	      If off-heap memory use is enabled, then --mem-offHeap-size must be positive"
)

parser.add_argument(
	"--mem-offHeap-size",
	type=str,
	dest="mem_offHeap_size",
	default = "16g",
	help="the absolute amount of memory in bytes which can be used for off-heap allocation.\
	      This must be set to a positive value when --mem-offHeap-enabled=true"
)

parser.add_argument(
	"--clean-checkpoints",
	type=bool,
	dest="clean_checkpoints",
	default = True,
	help="controls whether to clean checkpoint files if the reference is out of scope"
)

parser.add_argument(
	"--codegen-wholeStage",
	type=bool,
	dest="codegen_wholeStage",
	default = False,
	help="enable whole-stage code generation (experimental)"
)

parser.add_argument(
	"--codegen-fallback",
	type=bool,
	dest="codegen_fallback",
	default = True,
	help="when true, whole-stage codegen could be temporary disabled for the part of query that\
          fails to compile generated code"
)

parser.add_argument(
	"--broadcast-timeout",
	type=int,
	dest="broadcast_timeout",
	default = 1200,
	help="timeout in seconds for the broadcast wait time in broadcast joins"
)

parser.add_argument(
	"--networkTimeout",
	type=str,
	dest="network_timeout",
	default = "800s",
	help="default timeout for all network interactions"
)

if __name__ == "__main__":
	arguments = parser.parse_args()
	main(**vars(arguments))
