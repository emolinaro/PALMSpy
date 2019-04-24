class PColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


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
                                        "insert_missing": True,
                                        "insert_until": False,
                                        "insert_max_seconds": 600,
                                        "los_max_duration": 60
                                    },
                                    "filter_options": {
                                        "remove_lone": False,
                                        "filter_invalid_values": True,
                                        "max_speed":130,
                                        "max_ele_change":1000,
                                        "min_change_3_fixes":10
                                    },
                                    "trip_detection": {
                                        "detect_trip": True,
                                        "min_distance" : 34,
                                        "min_trip_length": 100,
                                        "min_trip_duration": 180,
                                        "min_pause_duration": 180,
                                        "max_pause_duration": 300
                                    },
                                    "mode_of_transportation": {
                                        "detect_trip_mode": True,
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
                                    "mark_not_wearing_time": True,
                                    "minutes_zeros_row": 30
                                },
                                "activity_bout":
                                {
                                    "detect_activity_bouts": True,
                                    "activity_bout_duration": 5,
                                    "activity_bout_upper_limit": 9999,
                                    "activity_bout_lower_limit": 1953,
                                    "activity_bout_tolerance": 2
                                },
                                "sedentary_bout":
                                {
                                    "detect_sedentary_bouts": True,
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
                            "merge_data_to_gps": True
                        }
                    }

    def dump_dict(self):

        return self.data

