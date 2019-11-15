import argparse
from tools import Metadata

meta = Metadata()
VER = meta.version
program = meta.name

# refer to the program name: %(prog)s
parser = argparse.ArgumentParser(
	prog=meta.name,
	usage='habitus --gps-path GPS_PATH --acc-path ACC_PATH [GPS options] [accelerometer options] [Spark options]',
	description="%(prog)s is an implementation of the Personal Activity and Location Measurement System (PALMS), written\
	             in Python and integrated with Apache Spark for cluster-computing.\
	             The program detects personal activity patterns of individual participants wearing\
                 a GPS data logger and a physical activity monitor.",
	formatter_class=argparse.ArgumentDefaultsHelpFormatter
)

requiredargs = parser.add_argument_group('required arguments')
gpsargs = parser.add_argument_group('GPS options')
accargs = parser.add_argument_group('accelerometer options')
mergeargs = parser.add_argument_group('merge options')
sparkargs = parser.add_argument_group('Spark options')

requiredargs.add_argument(
    "--gps-path",
    type=str,
    dest="gps_path",
	default="",
    help="directory of GPS raw data  %(default)s"
)

requiredargs.add_argument(
    "--acc-path",
    type=str,
    dest="acc_path",
	default="",
    help = "directory of accelerometer raw data  %(default)s"
)

parser.add_argument(
    "--config-file",
    default="",
    type=str,
	metavar='FILE',
    dest="config_file",
    help="load JSON file with configuration settings %(default)s"
)

parser.add_argument(
    "--version", "-v",
	action='version',
    version='%(prog)s v{}'.format(VER)
)

# GPS options
gpsargs.add_argument(
	"--interval",
	type=int,
	metavar='INT',
	dest="interval",
	default = 30,
	help="duration of interval between results in seconds"
)

gpsargs.add_argument(
	"--insert-missing",
	dest="insert_missing",
	action='store_true',
	help="gaps in GPS fixes are replaced by the last valid fix"
)

gpsargs.add_argument(
	"--insert-until",
	dest="insert_until",
	action='store_true',
	help="inserts until a max time (set by --insert-max-seconds) is reached;\
	      by default, inserts will be added until loss of signal time is reached"
)

gpsargs.add_argument(
	"--insert-max-seconds",
	type=int,
	metavar='INT',
	dest="insert_max_seconds",
	default = 600,
	help="max number of seconds to replace missing fixes with last valid fix (valid if --insert-until is enabled)"
)

gpsargs.add_argument(
	"--los-max-duration",
	type=int,
	metavar='INT',
	dest="los_max_duration",
	default = 600,
	help=" max number of seconds allowed to pass before loss of signal is declared"
)

gpsargs.add_argument(
	"--filter-invalid-values",
	dest="filter_invalid_values",
	action='store_true',
	help="remove invalid fixes"
)

gpsargs.add_argument(
	"--max-speed",
	type=int,
	metavar='INT',
	dest="max_speed",
	default = 130,
	help="fix is invalid if speed is greater than this value (in km/hr)"
)

gpsargs.add_argument(
	"--max-ele-change",
	type=int,
	metavar='INT',
	dest="max_ele_change",
	default = 1000,
	help="fix is invalid if elevation change is greater than this value (in meters)"
)

gpsargs.add_argument(
	"--min-change-3-fixes",
	type=int,
	metavar='INT',
	dest="min_change_3_fixes",
	default = 10,
	help="fix is invalid if change in distance between fix 1 and 3 is less than this value (in meters)"
)

gpsargs.add_argument(
	"--detect-trip",
	dest="detect_trip",
	action='store_true',
	help="calculate the fix trip type: STATIONARY (0), START POINT (1), MID POINT (2), PAUSE POINT (3),\
	      and END POINT (4)"
)

gpsargs.add_argument(
	"--min-distance",
	type=int,
	metavar='INT',
	dest="min_distance",
	default = 34,
	help="minimum distance (in meters) that must be travelled over one minute to indicate the start of a trip.\
	      Default value corresponds to a typical walking speed of 2 km/hr"
)

gpsargs.add_argument(
	"--min-trip-length",
	type=int,
	metavar='INT',
	dest="min_trip_length",
	default = 100,
	help="trips less than this distance (in meters) are not considered trips"
)

gpsargs.add_argument(
	"--min-trip-duration",
	type=int,
	metavar='INT',
	dest="min_trip_duration",
	default = 180,
	help="trips less than this duration (in seconds) are not considered trips"
)

gpsargs.add_argument(
	"--min-pause-duration",
	type=int,
	metavar='INT',
	dest="min_pause_duration",
	default = 180,
	help="when the duration at a location exceeds this value (in seconds), the point is marked as PAUSE POINT"
)

gpsargs.add_argument(
	"--max-pause-duration",
	type=int,
	metavar='INT',
	dest="max_pause_duration",
	default = 300,
	help=" when the duration of a pause exceeds this value (in seconds), the point is marked as an END POINT"
)

gpsargs.add_argument(
	"--detect-trip-mode",
	dest="detect_trip_mode",
	action='store_true',
	help="calculate the mode of transportation based on the speed: STATIONARY (0),\
	      PEDESTRIAN (1), BICYCLE (2), and VEHICLE (3)"
)

gpsargs.add_argument(
	"--vehicle-cutoff",
	type=int,
	metavar='INT',
	dest="vehicle_cutoff",
	default = 25,
	help="speeds greater than this value (in km/hr) will be marked as VEHICLE"
)

gpsargs.add_argument(
	"--bicycle-cutoff",
	type=int,
	metavar='INT',
	dest="bicycle_cutoff",
	default = 10,
	help="speeds greater than this value (in km/hr) will be marked as BICYCLE"
)

gpsargs.add_argument(
	"--walk-cutoff",
	type=int,
	metavar='INT',
	dest="walk_cutoff",
	default = 1,
	help="speeds greater than this value (in km/hr) will be marked as PEDESTRIAN"
)

gpsargs.add_argument(
	"--percentile-to-sample",
	type=int,
	metavar='INT',
	dest="percentile_to_sample",
	default = 90,
	help="speed comparisons are made at this percentile"
)

gpsargs.add_argument(
	"--min-segment-length",
	type=int,
	metavar='INT',
	dest="min_segment_length",
	default = 30,
	help="minimum length (in meters) of segments used to classify mode of transportation"
)

# Accelerometer options
accargs.add_argument(
	"--include-acc",
	dest="include_acc",
	action='store_true',
	help="append raw accelerometer data to the final output"
)

accargs.add_argument(
	"--include-vect",
	dest="include_vect",
	action='store_true',
	help="calculate the activity intensity from the accelerometer vector magnitude"
)

accargs.add_argument(
	"--mark-non-wearing",
	dest="mark_not_wearing_time",
	action='store_true',
	help="mark non-wearing time (set actvity count and activity intensity equal to -2)"
)

accargs.add_argument(
	"--minutes-zeros-row",
	type=int,
	metavar='INT',
	dest="minutes_zeros_row",
	default = 30,
	help="minimum non-wearing time (in minutes), corresponding to consecutive zeros in the activity count"
)

accargs.add_argument(
	"--detect-activity-bouts",
	dest="detect_activity_bouts",
	action='store_true',
	help="detect activity bouts"
)

accargs.add_argument(
	"--activity-bout-duration",
	type=int,
	metavar='INT',
	dest="activity_bout_duration",
	default = 5,
	help="minimum activity bout duration (in minutes)"
)

accargs.add_argument(
	"--activity-bout-up",
	type=int,
	metavar='INT',
	dest="activity_bout_upper_limit",
	default = 9999,
	help="activity bout upper limit"
)

accargs.add_argument(
	"--activity-bout-low",
	type=int,
	metavar='INT',
	dest="activity_bout_lower_limit",
	default = 1953,
	help="activity bout lower limit"
)

accargs.add_argument(
	"--activity-bout-tol",
	type=int,
	metavar='INT',
	dest="activity_bout_tolerance",
	default = 2,
	help="activity bout tolerance"
)

accargs.add_argument(
	"--detect-sedentary-bouts",
	dest="detect_sedentary_bouts",
	action='store_true',
	help="detect sedentary bouts"
)

accargs.add_argument(
	"--sedentary-bout-duration",
	type=int,
	metavar='INT',
	dest="sedentary_bout_duration",
	default = 30,
	help="sedentary bout duration (in minutes)"
)

accargs.add_argument(
	"--sedentary-bout-up",
	type=int,
	metavar='INT',
	dest="sedentary_bout_upper_limit",
	default = 100,
	help="sedentary bout upper limit"
)

accargs.add_argument(
	"--sedentary-bout-tol",
	type=int,
	metavar='INT',
	dest="sedentary_bout_tolerance",
	default = 1,
	help="sedentary bout tolerance"
)

accargs.add_argument(
	"--very-hard-cut",
	type=int,
	metavar='INT',
	dest="very_hard_cutoff",
	default = 9498,
	help="very hard activity cutoff value"
)

accargs.add_argument(
	"--hard-cut",
	type=int,
	metavar='INT',
	dest="hard_cutoff",
	default = 5725,
	help="hard activity cutoff value"
)

accargs.add_argument(
	"--moderate-cut",
	type=int,
	metavar='INT',
	dest="moderate_cutoff",
	default = 1953,
	help="moderate activity cutoff value"
)

accargs.add_argument(
	"--light-cut",
	type=int,
	metavar='INT',
	dest="light_cutoff",
	default = 100,
	help="light activity cutoff value"
)

# Merge options
mergeargs.add_argument(
	"--merge-acc-to-gps",
	dest="merge_data_to_gps",
	action='store_true',
	help="merge accelerometer data to GPS data and export them in one single file;\
	      by default, the processed accelerometer and GPS data will be saved in two different files"
)

mergeargs.add_argument(
	"--merge-gps-to-acc",
	dest="merge_data_to_acc",
	action='store_true',
	help="merge GPS data to accelerometer data and export them in one single file;\
	      by default, the processed accelerometer and GPS data will be saved in two different files"
)

# Spark options
sparkargs.add_argument(
	"--num-cores",
	type=str,
	metavar='INT',
	dest="num_cores",
	default = 8,
	help="total number of cores used in the calculation"
)
sparkargs.add_argument(
	"--driver-mem",
	type=str,
	metavar='STR',
	dest="driver_mem",
	default = '16g',
	help="amount of memory to use for the driver process,\
	      i.e. where SparkContext is initialized, in the same format as JVM memory strings\
	      with a size unit suffix (\"k\", \"m\", \"g\" or \"t\")"
)

sparkargs.add_argument(
	"--executor-mem",
	type=str,
	metavar='STR',
	dest="executor_mem",
	default = '16g',
	help="amount of memory to use per executor process, in the same format as JVM memory strings\
	      with a size unit suffix (\"k\", \"m\", \"g\" or \"t\")"
)

sparkargs.add_argument(
	"--mem-fraction",
	type=float,
	metavar='FLOAT',
	dest="mem_fraction",
	default = 0.6,
	help="expresses the size of the execution and storage memory as a fraction of the (JVM heap space - 300MB).\
          The rest of the space (40%%) is reserved for user data structures, internal metadata in Spark,\
          and safeguarding against OOM errors in the case of sparse and unusually large records"
)

sparkargs.add_argument(
	"--default-partitions",
	type=int,
	metavar='INT',
	dest="default_partitions",
	default = 20,
	help="Default number of partitions returned by transformations like join, reduceByKey, and parallelize"
)

sparkargs.add_argument(
	"--shuffle-partitions",
	type=int,
	metavar='INT',
	dest="shuffle_partitions",
	default = 20,
	help="configures the number of partitions to use when shuffling data for joins or aggregations"
)

sparkargs.add_argument(
	"--mem-offHeap-enabled",
	dest="mem_offHeap_enabled",
	action='store_false',
	help="if true, off-heap memory will be used for certain operations.\
	      If off-heap memory use is enabled, then --mem-offHeap-size must be positive"
)

sparkargs.add_argument(
	"--mem-offHeap-size",
	type=str,
	metavar='STR',
	dest="mem_offHeap_size",
	default = "16g",
	help="the absolute amount of memory in bytes which can be used for off-heap allocation.\
	      This must be set to a positive value when --mem-offHeap-enabled is true"
)

sparkargs.add_argument(
	"--clean-checkpoints",
	dest="clean_checkpoints",
	action='store_false',
	help="controls whether to clean checkpoint files if the reference is out of scope"
)

sparkargs.add_argument(
	"--codegen-wholeStage",
	dest="codegen_wholeStage",
	action='store_true',
	help="enable whole-stage code generation (experimental)"
)

sparkargs.add_argument(
	"--codegen-fallback",
	dest="codegen_fallback",
	action='store_false',
	help="when true, whole-stage codegen could be temporary disabled for the part of query that\
          fails to compile generated code"
)

sparkargs.add_argument(
	"--broadcast-timeout",
	type=int,
	metavar='INT',
	dest="broadcast_timeout",
	default = 1200,
	help="timeout in seconds for the broadcast wait time in broadcast joins"
)

sparkargs.add_argument(
	"--networkTimeout",
	type=str,
	metavar='STR',
	dest="network_timeout",
	default = "800s",
	help="default timeout for all network interactions"
)

#sparkargs.add_argument(
#	"--export-settings",
#	type=str,
#   metavar = 'STR',
#	dest="json_filename",
#	default = "settings.json",
#	help="save configuration parameters to JSON file"
#)
