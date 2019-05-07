class PColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class Metadata:
    name = 'HABITUS'
    version = '1.5'
    mantainer = 'Emiliano Molinaro'
    email = 'emil.molinaro@gmail.com'
    authors = 'Emiliano Molinaro'
    description = 'A program to detect personal activity patterns of individual participants wearing a GPS data logger and a physical activity monitor'

class Input:

    def __init__(self):
        """
            Initialize Spark, GPS, Accelerometer parameters
        """
        self.data = {
                        "spark":{
                            "driver":{
                                "memory": "16g"
                            },
                            "executor":{
                                "memory": "16g"
                            },
                            "memory":{
                                "fraction": "0.6",
                                "offHeap":{
                                    "enabled": True,
                                    "size": "16g"
                                }
                            },
                            "sql":{
                                "shuffle":{
                                    "partitions": "20"
                                },
                                "codegen":{
                                    "wholeStage": False,
                                    "fallback": True
                                },
                                "broadcastTimeout": "1200"
                            },
                            "cleaner":{
                                "referenceTracking":{
                                    "cleanCheckpoints": True
                                }
                            },
                            "network":{
                                "timeout": "800"
                            }
                        },
                        "gps": {
                            "parameters":
                                {

                                    "general": {
                                        "interval": 30,
                                        "insert_missing": False,
                                        "insert_until": False,
                                        "insert_max_seconds": 600,
                                        "los_max_duration": 600
                                    },
                                    "filter_options": {
                                        "remove_lone_fixes": True,
                                        "filter_invalid_values": False,
                                        "max_speed":130,
                                        "max_ele_change":1000,
                                        "min_change_3_fixes":10
                                    },
                                    "trip_detection": {
                                        "detect_trip": False,
                                        "min_distance" : 34,
                                        "min_trip_length": 100,
                                        "min_trip_duration": 180,
                                        "min_pause_duration": 180,
                                        "max_pause_duration": 300
                                    },
                                    "mode_of_transportation": {
                                        "detect_trip_mode": False,
                                        "vehicle_cutoff": 25,
                                        "bicycle_cutoff": 10,
                                        "walk_cutoff": 1,
                                        "percentile_to_sample": 90,
                                        "min_segment_length": 30
                                    }
                                }
                        },

                        "accelerometer":{
                            "parameters":
                            {
                                "include_acc": False,
                                "include_vect": False,
                                "not_wearing_time":
                                {
                                    "mark_not_wearing_time": False,
                                    "minutes_zeros_row": 30
                                },
                                "activity_bout":
                                {
                                    "detect_activity_bouts": False,
                                    "activity_bout_duration": 5,
                                    "activity_bout_upper_limit": 9999,
                                    "activity_bout_lower_limit": 1953,
                                    "activity_bout_tolerance": 2
                                },
                                "sedentary_bout":
                                {
                                    "detect_sedentary_bouts": False,
                                    "sedentary_bout_duration": 30,
                                    "sedentary_bout_upper_limit": 100,
                                    "sedentary_bout_tolerance": 1
                                },
                                "activity_classification":
                                {
                                    "very_hard_cutoff": 9498,
                                    "hard_cutoff": 5725,
                                    "moderate_cutoff": 1953,
                                    "light_cutoff": 100
                                }
                            }
                        },
                        "merge_options":
                        {
                            "merge_data_to_gps": False,
                            "merge_data_to_acc": False
                        }
                    }

    def dump_dict(self):

        return self.data

    def update(self,interval, insert_missing, insert_until,
         insert_max_seconds, loss_max_duration, filter_invalid_values,
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
         driver_mem, executor_mem, mem_fraction, shuffle_partitions, mem_offHeap_enabled,
         mem_offHeap_size, clean_checkpoints, codegen_wholeStage, codegen_fallback,
         broadcast_timeout, network_timeout):
        
        # GPS parameters
        self.data['gps']['parameters']['general']['interval'] = interval
        self.data['gps']['parameters']['general']['insert_missing'] = insert_missing
        self.data['gps']['parameters']['general']['insert_until'] = insert_until
        self.data['gps']['parameters']['general']['insert_max_seconds'] = insert_max_seconds
        self.data['gps']['parameters']['general']['los_max_duration'] = loss_max_duration
        self.data['gps']['parameters']['filter_options']['filter_invalid_values'] = filter_invalid_values
        self.data['gps']['parameters']['filter_options']['max_speed'] = max_speed
        self.data['gps']['parameters']['filter_options']['max_ele_change'] = max_ele_change
        self.data['gps']['parameters']['filter_options']['min_change_3_fixes'] = min_change_3_fixes
        self.data['gps']['parameters']['trip_detection']['detect_trip'] = detect_trip
        self.data['gps']['parameters']['trip_detection']['min_distance'] = min_distance
        self.data['gps']['parameters']['trip_detection']['min_trip_length'] = min_trip_length
        self.data['gps']['parameters']['trip_detection']['min_trip_duration'] = min_trip_duration
        self.data['gps']['parameters']['trip_detection']['min_pause_duration'] = min_pause_duration
        self.data['gps']['parameters']['trip_detection']['max_pause_duration'] = max_pause_duration
        self.data['gps']['parameters']['mode_of_transportation']['detect_trip_mode'] = detect_trip_mode
        self.data['gps']['parameters']['mode_of_transportation']['vehicle_cutoff'] = vehicle_cutoff
        self.data['gps']['parameters']['mode_of_transportation']['bicycle_cutoff'] = bicycle_cutoff
        self.data['gps']['parameters']['mode_of_transportation']['walk_cutoff'] = walk_cutoff
        self.data['gps']['parameters']['mode_of_transportation']['percentile_to_sample'] = percentile_to_sample
        self.data['gps']['parameters']['mode_of_transportation']['min_segment_length'] = min_segment_length

        # Accelerometer parameters
        self.data['accelerometer']['parameters']['include_acc'] = include_acc
        self.data['accelerometer']['parameters']['include_vect'] = include_vect
        self.data['accelerometer']['parameters']['not_wearing_time']['mark_not_wearing_time'] = mark_not_wearing_time
        self.data['accelerometer']['parameters']['not_wearing_time']['minutes_zeros_row'] = minutes_zeros_row
        self.data['accelerometer']['parameters']['activity_bout']['detect_activity_bouts'] = detect_activity_bouts
        self.data['accelerometer']['parameters']['activity_bout']['activity_bout_duration'] = activity_bout_duration
        self.data['accelerometer']['parameters']['activity_bout'][
            'activity_bout_upper_limit'] = activity_bout_upper_limit
        self.data['accelerometer']['parameters']['activity_bout'][
            'activity_bout_lower_limit'] = activity_bout_lower_limit
        self.data['accelerometer']['parameters']['activity_bout']['activity_bout_tolerance'] = activity_bout_tolerance
        self.data['accelerometer']['parameters']['sedentary_bout']['detect_sedentary_bouts'] = detect_sedentary_bouts
        self.data['accelerometer']['parameters']['sedentary_bout']['sedentary_bout_duration'] = sedentary_bout_duration
        self.data['accelerometer']['parameters']['sedentary_bout'][
            'sedentary_bout_upper_limit'] = sedentary_bout_upper_limit
        self.data['accelerometer']['parameters']['sedentary_bout']['sedentary_bout_tolerance'] = sedentary_bout_tolerance
        self.data['accelerometer']['parameters']['activity_classification']['very_hard_cutoff'] = very_hard_cutoff
        self.data['accelerometer']['parameters']['activity_classification']['hard_cutoff'] = hard_cutoff
        self.data['accelerometer']['parameters']['activity_classification']['moderate_cutoff'] = moderate_cutoff
        self.data['accelerometer']['parameters']['activity_classification']['light_cutoff'] = light_cutoff

        # Merge options
        self.data['merge_options']['merge_data_to_gps'] = merge_data_to_gps
        self.data['merge_options']['merge_data_to_acc'] = merge_data_to_acc

        # Spark parameters
        self.data['spark']['memory']['fraction'] = mem_fraction
        self.data['spark']['executor']['memory'] = executor_mem
        self.data['spark']['driver']['memory'] = driver_mem
        self.data['spark']['sql']['shuffle']['partitions'] = shuffle_partitions
        self.data['spark']['memory']['offHeap']['enabled'] = mem_offHeap_enabled
        self.data['spark']['memory']['offHeap']['size'] = mem_offHeap_size
        self.data['spark']['cleaner']['referenceTracking']['cleanCheckpoints'] = clean_checkpoints
        self.data['spark']['sql']['codegen']['wholeStage'] = codegen_wholeStage
        self.data['spark']['sql']['codegen']['fallback'] = codegen_fallback
        self.data['spark']['sql']['broadcastTimeout'] = broadcast_timeout
        self.data['spark']['network']['timeout'] = network_timeout

