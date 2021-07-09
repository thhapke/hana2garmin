###
# Parsing fit-files from GARMIN
# WARNING: Only zip-format supported as input
#
###


from os import path, listdir
import io
import zipfile
import gzip
import logging
from datetime import datetime


import yaml
import pandas as pd
from fitparse import FitFile, FitParseError
from hdbcli import dbapi

log_file = path.join('log/',"g2h_" + datetime.now().strftime("%Y%m%d_%H%M"))
logging.basicConfig(level=logging.INFO,handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ])

local_test = True
db_test = False
dump_csv = True

def save_data(sport,df,db):

    # For local testing only
    if local_test :
        return

    conn = dbapi.connect(address=db['host'],port=db['port'],user=db['user'],password=db['pwd'], encrypt=True,
                         sslValidateCertificate=False)
    cursor = conn.cursor()
    table = ''
    schema = db['schema']
    if sport == 'cycling_outdoor' :
        table = 'CYCLING_OUTDOOR' if db_test == False else 'CYCLING_OUTDOOR_TEST'
        data = df[["workout_id","date","timestamp","elapsed_time" ,"position_lat","position_long","gps_accuracy","enhanced_altitude","altitude",
	        "distance","heart_rate","cadence","enhanced_speed","speed","power","left_right_balance","grade",
	        "temperature","hr_zones","power_zones"]].values.tolist()
        sql = "UPSERT \"{}\".\"{}\" VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) WITH PRIMARY KEY;".format(schema,table)
    elif sport == 'cycling_indoor' :
        table ='CYCLING_INDOOR' if db_test == False else 'CYCLING_INDOOR_TEST'
        data = df[["workout_id","date", "timestamp", "elapsed_time", "heart_rate", "cadence", "power", "left_right_balance",
                   "temperature", "hr_zones", "power_zones"]].values.tolist()
        sql = "UPSERT \"{}\".\"{}\" VALUES(?,?,?,?,?,?,?,?,?,?,?) WITH PRIMARY KEY;".format(schema,table)
    elif sport == 'running' :
        table = 'RUNNING' if db_test == False else 'RUNNING_TEST'
        data = df[["workout_id","date", "timestamp", "elapsed_time", "position_lat","position_long","gps_accuracy",
                   "grade","vertical_speed","ascent",
                   "enhanced_altitude","altitude","distance","heart_rate", "cadence", "fractional_cadence",
                   "enhanced_speed", "speed", "vertical_oscillation","stance_time","stance_time_percent","temperature","hr_zones"]].values.tolist()
        sql = "UPSERT \"{}\".\"{}\" VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) WITH PRIMARY KEY;".format(schema,table)
    elif sport == 'swimming_pool' :
        table = 'SWIMMING_POOL' if db_test == False else 'SWIMMING_POOL_TEST'
        data = df[["workout_id","date", "timestamp", "elapsed_time","distance","total_cycles","heart_rate", "cadence",
                   "enhanced_speed", "speed","hr_zones"]].values.tolist()
        sql = "UPSERT \"{}\".\"{}\" VALUES(?,?,?,?,?,?,?,?,?,?,?) WITH PRIMARY KEY;".format(schema,table)
    elif sport == 'swimming_open_water' :
        table = 'SWIMMING_OPEN_WATER' if db_test == False else 'SWIMMING_OPEN_WATER_TEST'
        data = df[["workout_id","date","timestamp","elapsed_time","distance","position_lat","position_long","heart_rate",
                   "cadence","enhanced_speed", "speed","hr_zones"]].values.tolist()
        sql = "UPSERT \"{}\".\"{}\" VALUES(?,?,?,?,?,?,?,?,?,?,?,?) WITH PRIMARY KEY;".format(schema,table)
    elif sport == 'unidentified' :
        table = 'UNIDENTIFIED_SPORT' if db_test == False else 'UNIDENTIFIED_SPORT_TEST'
        data = df[["workout_id","date", "timestamp", "elapsed_time", "position_lat","position_long","gps_accuracy",
                   "enhanced_altitude","altitude","distance","heart_rate", "cadence", "enhanced_speed", "speed",
                   "power","left_right_balance","grade","temperature","hr_zones","power_zones","vertical_speed",
                   "ascent","fractional_cadence","vertical_oscillation","stance_time","stance_time_percent","total_cycles"]]\
            .values.tolist()
        sql = "UPSERT \"{}\".\"{}\" VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) WITH PRIMARY KEY;".format(schema,table)

    else :
        cursor.close()
        conn.close()
        raise ValueError("Unsupported sport")
    logging.info('Saving data to: {} ({})'.format(table,sql))
    cursor.executemany(sql, data)
    cursor.close()
    conn.close()

def check_column(df,col,fillchar) :
    try:
        df[col] = df[col].fillna(fillchar)
    except KeyError as kerr:
        logging.debug('Warning: data has not value for {}'.format(kerr))
        df[col] = 0

def check_columnNumeric(df,col,fillnum) :
    try:
        df[col] = pd.to_numeric(df[col],errors='coerce').fillna(fillnum)
    except KeyError as kerr:
        logging.debug('Warning: data has not value for {}'.format(kerr))
        df[col] = 0


def fit2df(bfile) :

    fitfile = FitFile(bfile)
    ### read all data and store in df

    # RECORDS
    records = [rec.get_values() for rec in fitfile.get_messages('record')]
    df = pd.DataFrame(records)

    # EVENT
    events = [rec.get_values() for rec in fitfile.get_messages('event')]
    df_events = pd.DataFrame(events)
    df_events = df_events.loc[
        (df_events['event'] == 'timer') & (df_events['event_type'] == 'start'), ['timestamp', 'event_type',
                                                                                 'timer_trigger']]
    df = df.merge(df_events, how='left', on='timestamp')
    df['elapsed_time'] = (df['timestamp'] - df['timestamp'].groupby(df['event_type'].eq('start').cumsum()).transform('first'))
    df['elapsed_time'] = df['elapsed_time'].dt.total_seconds()
    df.drop(columns = ['event_type','timer_trigger'],inplace=True)

    # HEARTRATE Zones
    hr_zone = [rec.get_values() for rec in fitfile.get_messages('hr_zone')]
    hr_zone_str = '-'.join([str(hr['high_bpm']) for hr in hr_zone]) if len(hr_zone) > 0 else ''
    df['hr_zones'] = hr_zone_str

    # POWER Zones
    power_zone = [rec.get_values() for rec in fitfile.get_messages('power_zone')]
    power_zone_str = '-'.join([str(p['high_value']) for p in power_zone]) if len(power_zone) > 0 else ''
    df['power_zones'] = power_zone_str

    # SPORT
    sportmsg = [rec.get_values() for rec in fitfile.get_messages('sport')]
    if len(sportmsg) > 1 :
        if sportmsg[0]['sport'] == 'cycling':
            sportmsg[0]['sub_sport'] = 'generic' if 'speed' in df.columns and df['speed'].max() > 0 else 'indoor'
        else :
            raise ValueError('More than 2 sports (except cycling) in file: {}'.format(sportmsg))

    # CYCLING
    if len(sportmsg) > 0 :
        sportmsg = sportmsg[0]
        if 'cycling' == sportmsg['sport'] :
            if 'indoor' in sportmsg['sub_sport'] :
                sport = 'cycling_indoor'
            elif 'generic' in sportmsg['sub_sport'] :
                sport = 'cycling_outdoor'
            else :
                raise ValueError('Unknown sub_sport of cycling'.format(sportmsg['sub_sport']))
        # ELSE
        elif 'running' == sportmsg['sport'] :
            sport = sportmsg['sport']
        elif 'swimming' == sportmsg['sport'] :
            if 'total_cycles' in df.columns and df['total_cycles'].max() > 1 :
                sport = 'swimming_pool'
            elif sportmsg['sub_sport'] == 'open_water' :
                sport = 'swimming_open_water'
            else :
                raise ValueError('Unsupported Swimming (not pool or open_water): {}'.format(sportmsg['sport']))
        else :
            raise ValueError('Unsupported sport: {}'.format(sportmsg['sport']))
    else :
        if "activity_type" in df.columns :
            if 'running' in df.activity_type.values :
                sport = 'running'
            else :
                logging.warning('Sport is not specified')
                raise ValueError('Sport not specified')
        else :
            logging.warning('Sport not indentified')
            sport = "unidentified"

    # WORKOUT_ID
    dtmin = df.timestamp.min()
    if dtmin == 0 :
        raise ValueError('Timestamp value: 0 ')
    df['workout_id'] =  int(dtmin.strftime('%Y%m%d%H%M'))
    df['date'] = dtmin.strftime('%Y-%m-%d')

    #Additional checks
    if sport == 'unidentified':
        # some checks:
        if 'distance' in df.columns :
            max_distance = df['distance'].max()
            max_et = df['elapsed_time'].max()
            av_v = max_distance/max_et  if max_et > 0 else 0
            av_speed = df['speed'].mean() if 'speed' in df.columns  else 0
            if max_distance > 0 and  'power' in df.columns and df['power'].max() > 0 :
                sport = 'cycling_outdoor'
                logging.info('Unidentified - identified: {}'.format(sport))
            elif max_distance > 0 and av_speed < 5.0 :
                sport = 'running'
                logging.info('Unidentified - identified: {}'.format(sport))

    ############ CYCLING OUTDOOR
    if sport == 'cycling_outdoor' :

        df = df.dropna(thresh=5)  # drops rows with too few values
        check_columnNumeric(df, 'heart_rate',0)
        check_columnNumeric(df, 'cadence', 0)
        check_columnNumeric(df, 'power', 0)
        check_columnNumeric(df, 'temperature', -273)
        check_columnNumeric(df, 'left_right_balance', 0)
        check_columnNumeric(df, 'position_lat', 0)
        check_columnNumeric(df, 'position_long', 0)
        check_columnNumeric(df, 'gps_accuracy', 0)
        check_columnNumeric(df, 'grade', 0)
        check_columnNumeric(df, 'enhanced_altitude', 0)
        check_columnNumeric(df, 'altitude', 0)
        check_columnNumeric(df, 'enhanced_speed', 0)
        check_columnNumeric(df, 'speed', 0)
        check_columnNumeric(df, 'elapsed_time', 0)
        check_columnNumeric(df, 'distance', 0)
        df = df.groupby('timestamp').max().reset_index()

    ############ CYCLING INDOOR
    elif sport == 'cycling_indoor' :
        #df = df.dropna(thresh=10)  # drops rows with too few values

        check_columnNumeric(df, 'heart_rate',0)
        check_columnNumeric(df, 'cadence', 0)
        check_columnNumeric(df, 'power', 0)
        check_columnNumeric(df, 'cadence', 0)
        check_columnNumeric(df, 'temperature', -273)
        check_columnNumeric(df, 'left_right_balance', 0)
        check_columnNumeric(df, 'elapsed_time', 0)
        check_column(df, 'hr_zones', '')
        check_column(df, 'power_zones', '')
        df = df.groupby('timestamp').max().reset_index()


    ############ RUNNING
    elif sport == 'running' :

        check_columnNumeric(df, 'heart_rate',0)
        check_columnNumeric(df, 'cadence', 0)
        check_columnNumeric(df, 'temperature', -273)
        check_columnNumeric(df, 'stance_time', 0)
        check_columnNumeric(df, 'stance_time_percent', 0)
        check_columnNumeric(df, 'vertical_oscillation', 0)
        check_columnNumeric(df, 'fractional_cadence', 0)
        check_columnNumeric(df, 'speed', 0)
        check_columnNumeric(df, 'enhanced_speed', 0)
        check_columnNumeric(df, 'vertical_speed', 0)
        check_columnNumeric(df, 'ascent', 0)
        check_columnNumeric(df, 'distance', 0)
        check_columnNumeric(df, 'enhanced_altitude', 0)
        check_columnNumeric(df, 'altitude', 0)
        check_columnNumeric(df, 'grade', 0)
        check_columnNumeric(df, 'position_lat', 0)
        check_columnNumeric(df, 'position_long', 0)
        check_columnNumeric(df, 'gps_accuracy', 0)
        check_columnNumeric(df, 'elapsed_time', 0)
        df = df.groupby('timestamp').max().reset_index()

    ############ Swimming POOL
    elif sport == 'swimming_pool' :
        check_columnNumeric(df, 'heart_rate', 0)
        check_columnNumeric(df, 'cadence', 0)
        check_columnNumeric(df, 'total_cycles', 0)
        check_columnNumeric(df, 'cadence', 0)
        check_columnNumeric(df, 'speed', 0)
        check_columnNumeric(df, 'enhanced_speed', 0)
        check_columnNumeric(df, 'distance', 0)
        check_columnNumeric(df, 'elapsed_time', 0)
        df = df.groupby('timestamp').max().reset_index()

    ############ Swimming OPEN WATER
    elif sport == 'swimming_open_water' :
        check_columnNumeric(df, 'heart_rate', 0)
        check_columnNumeric(df, 'cadence', 0)
        check_columnNumeric(df, 'position_lat', 0)
        check_columnNumeric(df, 'position_long', 0)
        check_columnNumeric(df, 'speed', 0)
        check_columnNumeric(df, 'enhanced_speed', 0)
        check_columnNumeric(df, 'distance', 0)
        check_columnNumeric(df, 'elapsed_time', 0)
        df = df.groupby('timestamp').max().reset_index()

    elif sport == 'unidentified' :
        check_columnNumeric(df, 'heart_rate',0)
        check_columnNumeric(df, 'cadence', 0)
        check_columnNumeric(df, 'power', 0)
        check_columnNumeric(df, 'temperature', -273)
        check_columnNumeric(df, 'left_right_balance', 0)
        check_columnNumeric(df, 'stance_time', 0)
        check_columnNumeric(df, 'stance_time_percent', 0)
        check_columnNumeric(df, 'vertical_oscillation', 0)
        check_columnNumeric(df, 'fractional_cadence', 0)
        check_columnNumeric(df, 'speed', 0)
        check_columnNumeric(df, 'enhanced_speed', 0)
        check_columnNumeric(df, 'vertical_speed', 0)
        check_columnNumeric(df, 'ascent', 0)
        check_columnNumeric(df, 'distance', 0)
        check_columnNumeric(df, 'enhanced_altitude', 0)
        check_columnNumeric(df, 'altitude', 0)
        check_columnNumeric(df, 'grade', 0)
        check_columnNumeric(df, 'position_lat', 0)
        check_columnNumeric(df, 'position_long', 0)
        check_columnNumeric(df, 'gps_accuracy', 0)
        check_columnNumeric(df, 'elapsed_time', 0)
        check_columnNumeric(df, 'total_cycles', 0)
        df = df.groupby('timestamp').max().reset_index()

    else :
        raise ValueError('Unknown sport not implemented'.format(sport))
    logging.info('*** {}  with #Records: {}'.format(sport,len(records)))

    return sport, df

def parse_save_fitfile(bfile,sports,db) :
    try:
        sport, df = fit2df(bfile)
        if sport in sports:
            save_data(sport, df, db)
        # Test output
        if dump_csv :
            fileout = path.join("/Users/Shared/data/triathlet/dump",sport + '.csv')
            if path.isfile(fileout) :
                df.to_csv(fileout,mode='a',index=False,header=False)
            else :
                df.to_csv(fileout, index=False, header=True)

    except ValueError as ve:
        logging.warning('Unsported sport or corrupt data: {}'.format(ve))
    except FitParseError as fp:
        logging.warning('Parse Error: {}'.format(fp))
    #except Exception as e:
    #    logging.warning('General Exception: {}'.format(e))
    #    raise Exception(e)

####### INPUT
def fitfile(inputfile,sports,db) :

    fileext = path.splitext(inputfile.filename)[1]
    ### Single File: GZ
    if fileext == '.gz':
        logging.info('Input GZ-File: {}'.format(inputfile.filename))
        gzf = gzip.open(inputfile).read()
        bfile = io.BytesIO(gzf)
        parse_save_fitfile(bfile, sports, db)
    ### Single File: FIT
    elif fileext == '.fit':
        logging.info('Input Fit-File: {}'.format(inputfile.filename))
        parse_save_fitfile(inputfile, sports, db)
    ### Multiple Files: ZIP
    elif fileext == '.zip':
        zip = zipfile.ZipFile(inputfile)
        fit_files = zip.namelist()
        fit_files = [f for f in fit_files if path.splitext(f)[1] in ['.fit', '.gz']]
        logging.info('Input File: {} with {} \'fit\'-files'.format(inputfile, len(fit_files)))

        for i, fit_file in enumerate(fit_files):


            logging.info('Parse file: {}'.format(fit_file))
            if path.splitext(fit_file)[1] in ['.gz']:
                gzfitfile = io.BytesIO(zip.read(fit_file))
                gzf = gzip.open(gzfitfile).read()
                bfile = io.BytesIO(gzf)
            else:
                bfile = io.BytesIO(zip.read(fit_file))
            parse_save_fitfile(bfile,sports,db)





if __name__ == '__main__' :

    logging.basicConfig(level=logging.INFO)

    # inputfile = "/Users/Shared/data/triathlet/OneDrive_2_11.3.2021.zip"
    inputfile = "/Users/Shared/data/triathlet/test/2019-04-22-110342-ELEMNT+97D8-314-12.zip"
    test_outputdir = "/Users/Shared/data/triathlet/csv"
    with open('config.yaml') as yamls:
        params = yaml.safe_load(yamls)

    db = {'host': params['HDB_HOST'],
          'user': params['HDB_USER'],
          'pwd': params['HDB_PWD'],
          'port': params['HDB_PORT']}

    sports = ['cycling_indoor','cycling_outdoor','running','swimming_pool', 'Swimming Pool','swimming_open_water']
    fitfile(inputfile,sports,db)