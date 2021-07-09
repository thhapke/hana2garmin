from datetime import datetime, timedelta
import math
import sys
import io
import logging
import pytz
import os
import pandas as pd
import numpy as np


att = {}
interval_width = 600
file = '/Users/Shared/data/triathlet/dump/bo2021.csv'
att['table_name'] = os.path.basename(file).split('.')[0].upper()
df = pd.read_csv(file,low_memory=False)
df = df[['TRAINING_ID',"date","timestamp","elapsed_time","distance","heart_rate","cadence","power","temperature"]]

col_map = {c:c.upper() for c in df.columns}
col_map['workout_id'] = 'TRAINING_ID'
col_map['heart_rate'] = 'HEARTRATE'

df = df.rename(columns = col_map)

####### same
df['TIMESTAMP'] = pd.to_datetime(df.TIMESTAMP)

# Time Series Index
df['INDEX'] = df['TIMESTAMP']
df = df.set_index('INDEX')
#df.tz_localize('utc')
#df = df.tz_convert(None)

# Rolling average over Power and find max power
df['POWER_RM'] = df.groupby(['TRAINING_ID'])['POWER'].rolling(interval_width).mean().droplevel(0)
df['POWER_MAX'] = df.groupby(['TRAINING_ID'])['POWER_RM'].transform(max)


# Timeindex of max power and get time intervals
max_best = df[(df['POWER_RM'] == df['POWER_MAX']) & (df['POWER_MAX']>0)].index.values
best_i = np.vstack((max_best - np.timedelta64(interval_width, 's'), max_best)).T

num_trainings = len(df['TRAINING_ID'].unique())

#log('# best intervals: {}  Number of trainings: {}'.format(len(best_i),num_trainings))

best_list = list()
milestone_counter = len(best_i) // 10
for counter, bi in enumerate(best_i):
    dfi = df[(df.index > bi[0]) & (df.index <= bi[1])]
    best_list.append(dfi)
    if counter % milestone_counter == 0:
        print('Processed: {}/{}'.format(counter, len(best_i)))

tdf = pd.concat(best_list)


aggreg = {'DATE': 'first', 'TIMESTAMP': ['min', 'max'], \
          'POWER': ['min', 'max', 'mean'], 'HEARTRATE': ['min', 'max', 'mean'], 'CADENCE': ['min', 'max', 'mean']}

tdf = tdf.groupby('TRAINING_ID').agg(aggreg).reset_index()
tdf.columns = ['_'.join(col).upper() for col in tdf.columns]

tdf['SPORT_TYPE'] = att['table_name']
tdf.rename(columns={"TRAINING_ID_": "TRAINING_ID", "DATE_FIRST": "DATE"}, inplace=True)

tdf['DURATION'] = (tdf['TIMESTAMP_MAX'] - tdf['TIMESTAMP_MIN']).dt.total_seconds()
tdf['TIMESTAMP_START'] = tdf['TIMESTAMP_MIN'].dt.strftime('%Y-%m-%d %H:%M:%S')
tdf['TIMESTAMP_END'] = tdf['TIMESTAMP_MAX'].dt.strftime('%Y-%m-%d %H:%M:%S')
tdf['INTERVAL_WIDTH'] = interval_width

# cast
tdf['DURATION'] = tdf['DURATION'].astype('int')
tdf['HEARTRATE_MIN'] = tdf['HEARTRATE_MIN'].astype('int')
tdf['HEARTRATE_MAX'] = tdf['HEARTRATE_MAX'].astype('int')
tdf['HEARTRATE_MEAN'] = tdf['HEARTRATE_MEAN'].astype('int')
tdf['CADENCE_MIN'] = tdf['CADENCE_MIN'].astype('float')
tdf['CADENCE_MAX'] = tdf['CADENCE_MAX'].astype('float')
tdf['CADENCE_MEAN'] = tdf['CADENCE_MEAN'].astype('float')
tdf['POWER_MIN'] = tdf['POWER_MIN'].astype('float')
tdf['POWER_MAX'] = tdf['POWER_MAX'].astype('float')
tdf['POWER_MEAN'] = tdf['POWER_MEAN'].astype('float')
tdf['INTERVAL_WIDTH'] = tdf['INTERVAL_WIDTH'].astype('int')

# sort dataframe according to target table
tdf = tdf[['TRAINING_ID', 'DATE', 'SPORT_TYPE', 'INTERVAL_WIDTH', 'TIMESTAMP_START', 'TIMESTAMP_END', \
           'POWER_MIN', 'POWER_MAX', 'POWER_MEAN', 'HEARTRATE_MIN', 'HEARTRATE_MAX', \
           'HEARTRATE_MEAN', 'CADENCE_MIN', 'CADENCE_MAX', 'CADENCE_MEAN']]

att["table"] = {"columns": [
    {"class": str(tdf[tdf.columns[0]].dtype), "tdf_name": tdf.columns[0], "name": "TRAINING_ID", "nullable": False,
     "type": {"hana": "BIGINT"}},
    {"class": str(tdf[tdf.columns[1]].dtype), "tdf_name": tdf.columns[1], "name": "DATE", "nullable": True,
     "type": {"hana": "DAYDATE"}},
    {"class": str(tdf[tdf.columns[2]].dtype), "tdf_name": tdf.columns[2], "name": "SPORT_TYPE", "nullable": False,
     "size": 25, "type": {"hana": "NVARCHAR"}},
    {"class": str(tdf[tdf.columns[3]].dtype), "tdf_name": tdf.columns[3], "name": "INTERVAL_WIDTH", "nullable": True,
     "type": {"hana": "INTEGER"}},
    {"class": str(tdf[tdf.columns[4]].dtype), "tdf_name": tdf.columns[4], "name": "TIMESTAMP_START", "nullable": True,
     "type": {"hana": "LONGDATE"}},
    {"class": str(tdf[tdf.columns[5]].dtype), "tdf_name": tdf.columns[5], "name": "TIMESTAMP_END", "nullable": True,
     "type": {"hana": "LONGDATE"}},
    {"class": str(tdf[tdf.columns[6]].dtype), "tdf_name": tdf.columns[6], "name": "POWER_MIN", "nullable": True,
     "type": {"hana": "DOUBLE"}},
    {"class": str(tdf[tdf.columns[7]].dtype), "tdf_name": tdf.columns[7], "name": "POWER_MAX", "nullable": True,
     "type": {"hana": "DOUBLE"}},
    {"class": str(tdf[tdf.columns[8]].dtype), "tdf_name": tdf.columns[8], "name": "POWER_MEAN", "nullable": True,
     "type": {"hana": "DOUBLE"}},
    {"class": str(tdf[tdf.columns[9]].dtype), "tdf_name": tdf.columns[9], "name": "HEARTRATE_MIN", "nullable": True,
     "type": {"hana": "INTEGER"}},
    {"class": str(tdf[tdf.columns[10]].dtype), "tdf_name": tdf.columns[10], "name": "HEARTRATE_MAX", "nullable": True,
     "type": {"hana": "INTEGER"}},
    {"class": str(tdf[tdf.columns[11]].dtype), "tdf_name": tdf.columns[11], "name": "HEARTRATE_MEAN", "nullable": True,
     "type": {"hana": "INTEGER"}},
    {"class": str(tdf[tdf.columns[12]].dtype), "tdf_name": tdf.columns[12], "name": "CADENCE_MIN", "nullable": True,
     "type": {"hana": "DOUBLE"}},
    {"class": str(tdf[tdf.columns[13]].dtype), "tdf_name": tdf.columns[13], "name": "CADENCE_MAX", "nullable": True,
     "type": {"hana": "DOUBLE"}},
    {"class": str(tdf[tdf.columns[14]].dtype), "tdf_name": tdf.columns[14], "name": "CADENCE_MEAN", "nullable": True,
     "type": {"hana": "DOUBLE"}}], "name": "BEST_INTERVAL", "version": 1}

#data = tdf.values.tolist()

tdf.to_csv('/Users/Shared/data/triathlet/dump/best_interval.csv',index = False)







