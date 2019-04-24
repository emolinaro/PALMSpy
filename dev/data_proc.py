#!/usr/bin/env python3
# coding: utf-8

# # Analisys PALMS Data Set

# ## Data Processing


import os
import pandas as pd
import numpy as np
import datetime as dt
import time


def read_gps_data(path):

    columns = pd.read_csv(path, sep=',').columns.values.tolist() # extract headers
    columns = [columns[k].strip() for k in range(len(columns))] # remove initial/final spaces

    # remove last empty column
    gps_df = pd.read_csv(path, sep=',', names=columns, low_memory=False).drop(columns=['INDEX'])

    # remove duplicate lines to get rid of multiple header rows
    gps_df = gps_df.drop_duplicates()

    # remove the first row which contains a repetition of the header
    gps_df = gps_df.drop(gps_df.index[0])
    gps_df = gps_df.reset_index()
    gps_df = gps_df.drop(columns=['index'])
    
    try:
        gps_df['UTC DATE'] = [gps_df['UTC DATE'][k].strip() for k in range(len(gps_df))] # remove initial/final spaces
        gps_df['UTC TIME'] = [gps_df['UTC TIME'][k].strip() for k in range(len(gps_df))] # remove initial/final spaces
        gps_df['LOCAL DATE'] = [gps_df['LOCAL DATE'][k].strip() for k in range(len(gps_df))] # remove initial/final spaces
        gps_df['LOCAL TIME'] = [gps_df['LOCAL TIME'][k].strip() for k in range(len(gps_df))] # remove initial/final spaces
        # add datetime column
        gps_df['DATETIME'] = pd.to_datetime(gps_df['UTC DATE'] + " " + gps_df['UTC TIME'], format="%Y/%m/%d")
    except:
        try:
            gps_df['DATE'] = [gps_df['DATE'][k].strip() for k in range(len(gps_df))] # remove initial/final spaces
            gps_df['TIME'] = [gps_df['TIME'][k].strip() for k in range(len(gps_df))] # remove initial/final spaces
            # add datetime column
            gps_df['DATETIME'] = pd.to_datetime(gps_df['DATE'] + " " + gps_df['TIME'], format="%Y/%m/%d")
        except:
            gps_df['UTC'] = [gps_df['UTC'][k].strip() for k in range(len(gps_df))] # remove initial/final spaces
            gps_df['LOCAL TIME'] = [gps_df['LOCAL TIME'][k].strip() for k in range(len(gps_df))] # remove initial/final spaces
            # add datetime column
            gps_df['DATETIME'] = pd.to_datetime(gps_df['UTC'], format="%Y/%m/%d")

    return gps_df

def read_acc_data(path):
    # Read accelerometer data

    acc_list = [line.rstrip('\n') for line in open(path)]

    # extract metadata

    start_time = acc_list[2].split()[2]
    start_date = acc_list[3].split()[2]
    interval = acc_list[4].split()[3]
    end_time = acc_list[5].split()[2]
    end_date = acc_list[6].split()[2]

    try:
        start_timestamp = dt.datetime.strptime(start_date + " " + start_time, '%d/%m/%Y %H:%M:%S')
    except:
        try:
            start_timestamp = dt.datetime.strptime(start_date + " " + start_time, '%m/%d/%Y %H:%M:%S')
        except:
            start_timestamp = dt.datetime.strptime(start_date + " " + start_time, '%Y/%m/%d %H:%M:%S')

    x = time.strptime(interval, '%H:%M:%S')
    interval = dt.timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec)
    try:
        end_timestamp = dt.datetime.strptime(end_date + " " + end_time, '%d-%b-%y %H:%M:%S')
    except:
        try:
            end_timestamp = dt.datetime.strptime(end_date + " " + end_time, '%d-%m-%Y %H:%M:%S')
        except:
            try:
                end_timestamp = dt.datetime.strptime(end_date + " " + end_time, '%d/%m/%Y %H:%M:%S')
            except:
                try:
                    end_timestamp = dt.datetime.strptime(end_date + " " + end_time, '%m/%d/%Y %H:%M:%S')
                except:
                    end_timestamp = dt.datetime.strptime(end_date + " " + end_time, '%Y/%m/%d %H:%M:%S')

    if len(acc_list[10]) < 50: 
        acc_data = acc_list[10:]
    else:
        acc_data = acc_list[11:]
        
    tot_intervals = len(acc_data)

    # create accelerometer dataframe

    acc_df = pd.DataFrame({})
    acc_df['DATETIME'] = [start_timestamp + k*interval for k in range(tot_intervals)]
    acc_df['ACC DATA'] = acc_data
    
    return (interval, acc_df)

def merge_data(work_dir, list_file_acc, list_file_gps, index_file):
    
    acc = "acc/"  # accelerometer data (metadata)
    gps = "gps/"  # GPS data (csv format)
    
    
    # read gps data
    path = work_dir + gps + list_file_gps[index_file]
    gps_df = read_gps_data(path)
    
    # read accelerometer data
    path = work_dir + acc + list_file_acc[index_file]
    
    interval, acc_df = read_acc_data(path)
    
    # select gps data based on accelerometer timestamps
    w1 = np.zeros(len(gps_df))
    w2 = gps_df['DATETIME'].copy()
    for i in range(len(acc_df)):
        w1 = abs((gps_df['DATETIME'] - acc_df['DATETIME'][i]).dt.total_seconds()) < interval.total_seconds()/2 # only one element equal to 1
        if w1.sum() == 1:  # found a value of gps_df which matches timestamp in acc_df
            w2[w1] = acc_df['DATETIME'][i]
            
    gps_df2 = gps_df.copy()
    gps_df2['DATETIME'] = w2
    merged_df = pd.merge(acc_df, gps_df2, on='DATETIME')
    
    return merged_df


def main():
    
    # select data directory

    work_dir = input("\n Introduce data path: ")
    if work_dir[-1] != "/":
        work_dir = work_dir + "/"
    
    acc = "acc/"  # accelerometer data (metadata)
    gps = "gps/"  # GPS data (csv format)
    merge = "merge/"
    
    # acc and GPS data files have the same name
    list_file_acc = sorted(os.listdir(work_dir + acc))
    list_file_gps = sorted(os.listdir(work_dir + gps))
    list_file_merge = sorted([list_file_acc[k][:-4] + "-merged.csv" for k in range(len(list_file_acc))]) # list of output files
    
    
    # set output file directory
    try:
        os.mkdir(work_dir + merge)
        print("\n Output directory: " + work_dir + merge)
        list_file_merge_ = []
    except FileExistsError:
        print("\n Output directory: " + work_dir + merge)
        list_file_merge_ = os.listdir(work_dir + merge)
    
    print("")
    for file in list_file_merge:  
        
        if file in list_file_merge_:
            continue # check if the output file already exists
        
        index_file = list_file_merge.index(file)
        print("      Calculating merged dataframe from " + list_file_acc[index_file] + " and " + list_file_gps[index_file] + "...")
        try:
            merged_df = merge_data(work_dir, list_file_acc, list_file_gps, index_file)
        except:
            print("        [____> error: check generation dataset in " + file + "\n")
            continue
            
        merged_df.to_csv(work_dir + merge + file)
        print("      Output file " + file + " added.\n")
    
    print("All dataset processed.")
    
    
if __name__ == "__main__":
    main()