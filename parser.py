import argparse
from tools import Metadata

meta = Metadata()
file = open("VERSION", "r")
VER = meta.version
program = meta.name

# refer tot he program name: %(prog)s
parser = argparse.ArgumentParser(
	prog=meta.name,
	usage='habitus --gps-path GPS_PATH --acc-path ACC_PATH [GPS options] [accelerometer options] [Spark options]',
	description="%(prog)s is an implementation of the Personal Activity and Location Measurement System (PALMS)\
	             with Apache Spark. The program detects personal activity patterns of individual participants wearing\
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
	dest="interval",
	default = 30,
	help="duration of interval between results in seconds"
)

gpsargs.add_argument(
	"--insert-missing",
	type=bool,
	dest="insert_missing",
	default = True,
	help="if true, gaps in GPS fixes are replaced by the last valid fix"
)

gpsargs.add_argument(
	"--insert-until",
	type=bool,
	dest="insert_until",
	default = False,
	help="if true, inserts until a max time (set by --insert-max-seconds) is reached.\
	      If false, inserts will be added until loss of signal time is reached"
)

gpsargs.add_argument(
	"--insert-max-seconds",
	type=int,
	dest="insert_max_seconds",
	default = 600,
	help="max number of seconds to replace missing fixes with last valid fix (valid if --insert-until=true)"
)

gpsargs.add_argument(
	"--los-max-duration",
	type=int,
	dest="los_max_duration",
	default = 60,
	help=" max number of minutes allowed to pass before loss of signal is declared"
)

gpsargs.add_argument(
	"--filter-invalid-values",
	type=bool,
	dest="filter_invalid_values",
	default = True,
	help="if true, removes invalid fixes"
)

gpsargs.add_argument(
	"--max-speed",
	type=int,
	dest="max_speed",
	default = 130,
	help="fix is invalid if speed is greater than this value (in km/hr)"
)

gpsargs.add_argument(
	"--max-ele-change",
	type=int,
	dest="max_ele_change",
	default = 1000,
	help="fix is invalid if elevation change is greater than this value (in meters)"
)

gpsargs.add_argument(
	"--min-change-3-fixes",
	type=int,
	dest="min_change_3_fixes",
	default = 10,
	help="fix is invalid if change in distance between fix 1 and 3 is less than this value (in meters)"
)

gpsargs.add_argument(
	"--detect-trip",
	type=bool,
	dest="detect_trip",
	default = True,
	help="if true, calculates the fix trip type: STATIONARY (0), START POINT (1), MID POINT (2), PAUSE POINT (3),\
	      and END POINT (4)"
)

gpsargs.add_argument(
	"--min-distance",
	type=int,
	dest="min_distance",
	default = 34,
	help="minimum distance (in meters) that must be travelled over one minute to indicate the start of a trip.\
	      Default value corresponds to a typical walking speed of 2 Km/hr"
)

gpsargs.add_argument(
	"--min-trip-length",
	type=int,
	dest="min_trip_length",
	default = 100,
	help="trips less than this distance (in meters) are not considered trips"
)

gpsargs.add_argument(
	"--min-trip-duration",
	type=int,
	dest="min_trip_duration",
	default = 180,
	help="trips less than this duration (in seconds) are not considered trips"
)

gpsargs.add_argument(
	"--min-pause-duration",
	type=int,
	dest="min_pause_duration",
	default = 180,
	help="when the duration at a location exceeds this value (in seconds), the point is marked as PAUSE POINT"
)

gpsargs.add_argument(
	"--max-pause-duration",
	type=int,
	dest="max_pause_duration",
	default = 300,
	help=" when the duration of a pause exceeds this value (in seconds), the point is marked as an END POINT"
)

gpsargs.add_argument(
	"--detect-trip-mode",
	type=bool,
	dest="detect_trip_mode",
	default = True,
	help="if true, calculates the mode of transportation based on the speed: STATIONARY (0),\
	      PEDESTRIAN (1), BICYCLE (2), and VEHICLE (3)"
)

gpsargs.add_argument(
	"--vehicle-cutoff",
	type=int,
	dest="vehicle_cutoff",
	default = 25,
	help="speeds greater than this value (in Km/hr) will be marked as VEHICLE"
)

gpsargs.add_argument(
	"--bicycle-cutoff",
	type=int,
	dest="bicycle_cutoff",
	default = 10,
	help="speeds greater than this value (in Km/hr) will be marked as BICYCLE"
)

gpsargs.add_argument(
	"--walk-cutoff",
	type=int,
	dest="walk_cutoff",
	default = 1,
	help="speeds greater than this value (in Km/hr) will be marked as PEDESTRIAN"
)

gpsargs.add_argument(
	"--percentile-to-sample",
	type=int,
	dest="percentile_to_sample",
	default = 90,
	help="speed comparisons are made at this percentile"
)

gpsargs.add_argument(
	"--min-segment-length",
	type=int,
	dest="min_segment_length",
	default = 30,
	help="minimum length (in meters) of segments used to classify mode of transportation"
)

# Accelerometer options
accargs.add_argument(
	"--include-acc",
	type=bool,
	dest="include_acc",
	default = False,
	help="if true, all measured accelerometer data are attached to the final output"
)

accargs.add_argument(
	"--include-vect",
	type=bool,
	dest="include_vect",
	default = False,
	help="if true, the activity intensity is calculated from the accelerometer vector magnitude"
)

accargs.add_argument(
	"--mark-not-wearing",
	type=bool,
	dest="mark_not_wearing_time",
	default = True,
	help="if true, it will mark not-wearing time (set actvity count and activity intensity equal to -2)"
)

accargs.add_argument(
	"--minutes-zeros-row",
	type=int,
	dest="minutes_zeros_row",
	default = 30,
	help="minimum not-wearing time, corresponding to consecutive zeros in the activity count"
)

accargs.add_argument(
	"--detect-activity-bouts",
	type=bool,
	dest="detect_activity_bouts",
	default = True,
	help="if true, it will detect activity bouts"
)

accargs.add_argument(
	"--activity-bout-duration",
	type=int,
	dest="activity_bout_duration",
	default = 5,
	help="minimum activity bout duration (in minutes)"
)

accargs.add_argument(
	"--activity-bout-up",
	type=int,
	dest="activity_bout_upper_limit",
	default = 9999,
	help="activity bout upper limit"
)

accargs.add_argument(
	"--activity-bout-low",
	type=int,
	dest="activity_bout_lower_limit",
	default = 1953,
	help="activity bout lower limit"
)

accargs.add_argument(
	"--activity-bout-tol",
	type=int,
	dest="activity_bout_tolerance",
	default = 2,
	help="activity bout tolerance"
)

accargs.add_argument(
	"--detect-sedentary-bouts",
	type=bool,
	dest="detect_sedentary_bouts",
	default = True,
	help="if true, it will detect sedentary bouts"
)

accargs.add_argument(
	"--sedentary-bout-duration",
	type=int,
	dest="sedentary_bout_duration",
	default = 30,
	help="sedentary bout duration (in minutes)"
)

accargs.add_argument(
	"--sedentary-bout-up",
	type=int,
	dest="sedentary_bout_upper_limit",
	default = 100,
	help="sedentary bout upper limit"
)

accargs.add_argument(
	"--sedentary-bout-tol",
	type=int,
	dest="sedentary_bout_tolerance",
	default = 1,
	help="sedentary bout tolerance"
)

accargs.add_argument(
	"--very-hard-cut",
	type=int,
	dest="very_hard_cutoff",
	default = 9498,
	help="very hard activity cutoff value"
)

accargs.add_argument(
	"--hard-cut",
	type=int,
	dest="hard_cutoff",
	default = 9498,
	help="hard activity cutoff value"
)

accargs.add_argument(
	"--moderate-cut",
	type=int,
	dest="moderate_cutoff",
	default = 1953,
	help="moderate activity cutoff value"
)

accargs.add_argument(
	"--light-cut",
	type=int,
	dest="light_cutoff",
	default = 100,
	help="light activity cutoff value"
)

# Merge options
mergeargs.add_argument(
	"--merge-acc-to-gps",
	type=bool,
	dest="merge_data_to_gps",
	default = True,
	help="if true, the accelerometer data will be merged to the GPS data and exported in one single file.\
	      If, false, the processed accelerometer and GPS data will be saved in two different files"
)

# Spark options
sparkargs.add_argument(
	"--mem-fraction",
	type=float,
	dest="mem_fraction",
	default = 0.6,
	help="expresses the size of the execution and storage memory a fraction of the (JVM heap space - 300MB).\
          The rest of the space (40%%) is reserved for user data structures, internal metadata in Spark,\
          and safeguarding against OOM errors in the case of sparse and unusually large records"
)

sparkargs.add_argument(
	"--executor-mem",
	type=str,
	dest="executor_mem",
	default = '16g',
	help="amount of memory to use per executor process, in the same format as JVM memory strings\
	      with a size unit suffix (\"k\", \"m\", \"g\" or \"t\")"
)

sparkargs.add_argument(
	"--driver-mem",
	type=str,
	dest="driver_mem",
	default = '16g',
	help="amount of memory to use for the driver process,\
	      i.e. where SparkContext is initialized, in the same format as JVM memory strings\
	      with a size unit suffix (\"k\", \"m\", \"g\" or \"t\")"
)

sparkargs.add_argument(
	"--shuffle-partitions",
	type=int,
	dest="shuffle_partitions",
	default = 20,
	help="configures the number of partitions to use when shuffling data for joins or aggregations"
)

sparkargs.add_argument(
	"--mem-offHeap-enabled",
	type=bool,
	dest="mem_offHeap_enabled",
	default = True,
	help="if true, off-heap memory for ill be used for certain operations.\
	      If off-heap memory use is enabled, then --mem-offHeap-size must be positive"
)

sparkargs.add_argument(
	"--mem-offHeap-size",
	type=str,
	dest="mem_offHeap_size",
	default = "16g",
	help="the absolute amount of memory in bytes which can be used for off-heap allocation.\
	      This must be set to a positive value when --mem-offHeap-enabled=true"
)

sparkargs.add_argument(
	"--clean-checkpoints",
	type=bool,
	dest="clean_checkpoints",
	default = True,
	help="controls whether to clean checkpoint files if the reference is out of scope"
)

sparkargs.add_argument(
	"--codegen-wholeStage",
	type=bool,
	dest="codegen_wholeStage",
	default = False,
	help="enable whole-stage code generation (experimental)"
)

sparkargs.add_argument(
	"--codegen-fallback",
	type=bool,
	dest="codegen_fallback",
	default = True,
	help="when true, whole-stage codegen could be temporary disabled for the part of query that\
          fails to compile generated code"
)

sparkargs.add_argument(
	"--broadcast-timeout",
	type=int,
	dest="broadcast_timeout",
	default = 1200,
	help="timeout in seconds for the broadcast wait time in broadcast joins"
)

sparkargs.add_argument(
	"--networkTimeout",
	type=str,
	dest="network_timeout",
	default = "800s",
	help="default timeout for all network interactions"
)

#sparkargs.add_argument(
#	"--export-settings",
#	type=str,
#	dest="json_filename",
#	default = "settings.json",
#	help="save configuration parameters to JSON file"
#)