#%% Imports
import pandas as pd
import pickle
import os

#%% Internal Functions
def read_bbg_pickle(fname):
    with open(fname, 'rb') as f:
        f = pickle.load(f, encoding='latin1')

    return f['ts'].copy(), f['meta'].copy()


def import_sec(schema_fn, sec):
    ts = {}
    meta = {}
    for fld_out, (src_file, src_fld) in schema_fn(sec).items():
        try:
            ts_, meta_ = read_bbg_pickle(src_file)

            ts[fld_out] = ts_.loc[:, src_fld]
            meta[fld_out] = meta_
        except:
            pass #print('error')

    out = {'ts':   pd.concat(ts.values(), axis=1, keys=ts.keys()),
           'meta': pd.concat(meta.values(), axis=0).drop_duplicates()}

    return out


def import_securities(seclist, schema_fn):
    return {sec: import_sec(schema_fn, sec) for sec in seclist}

def slice_d(d, keys):
    return {k:v for k,v in d.items() if k in keys}


def get_root(fut_tckr):
    return fut_tckr.split('.')[0]


def get_partition_futures(bbgDB):

    futs = list(bbgDB['Futures'].keys())

    unique_roots = list(set([get_root(f) for f in futs]))
    get_tckrs_by_root = lambda futs, root: [f for f in futs if get_root(f)==root]

    return {root + '.pkl': get_tckrs_by_root(futs, root) for root in unique_roots}


def get_partition_by_sec(bbgDB, db):
    return {sec + '.pkl': sec for sec in bbgDB[db].keys()}


def write_data(db, dbFldr, dbName, partition):
    for file_out, sec_list in partition.items():

        print('Exporting {} Data'.format(dbName))
        sec_slice = slice_d(db, sec_list)

        ts = {k: v['ts']   for k,v in sec_slice.items()}

        fname = dbFldr + dbName + '/' + file_out
        with open(fname, 'wb') as f:
            print(' ...writing file {}'.format(fname))
            pickle.dump(ts, f)


def compile_meta(db, dbName, write=True):
    def validate_meta_dtypes(s):
        checks = {'LAST_TRADEABLE_DT': pd._libs.tslibs.timestamps.Timestamp,
                  'FUT_DLV_DT_FIRST':  pd._libs.tslibs.timestamps.Timestamp,
                  'FUT_NOTICE_FIRST':  pd._libs.tslibs.timestamps.Timestamp}

        for fld, cls in checks.items():
            if fld in s:
                if not isinstance(s[fld], cls):
                    return

        if any(s.index.duplicated()):
            return

        return s

    print('...writing meta data for {}'.format(dbName))
    sec_slice = slice_d(db, list(db.keys()))

    meta = pd.concat([validate_meta_dtypes(sec['meta']) for tckr, sec in sec_slice.items()], axis=1, keys=sec_slice.keys(), sort=False).transpose()

    if write:
        meta.to_csv(OUTPUT_FOLDER + dbName + '/_meta.csv')

    return meta

#%% DB Parameters / Config
def get_db_params(db):

    if db == 'Futures':
        seclist = [f.split('.')[0] + '.' + f.split('.')[1] for f in os.listdir(BLOOMBERG_RAW_DB + db + '/') if f.endswith('.pickle')]
        return seclist, schema_futures

    if db == 'Futures_gen':
        seclist = [f.split('.')[0] + '.' + f.split('.')[1] for f in os.listdir(BLOOMBERG_RAW_DB + db + '/') if f.endswith('.pickle')]
        return seclist, schema_futgen

    if db == 'FX':
        seclist = [f.split('.')[0] for f in os.listdir(BLOOMBERG_RAW_DB + db + '/') if f.endswith('.pickle')]
        seclist = list(set(seclist))
        return seclist, schema_fx

    if db == 'Index':
        seclist = [f.replace('.pickle','') for f in os.listdir(BLOOMBERG_RAW_DB + db + '/') if f.endswith('.pickle')]
        return seclist, schema_index

    if db == 'InterestRates':
        seclist = [f.split('.')[0] for f in os.listdir(BLOOMBERG_RAW_DB + db + '/') if f.endswith('.pickle')]
        seclist = list(set(seclist))
        return seclist, schema_interest_rates

    if db == 'CoT':
        seclist = [f.split('.')[0] for f in os.listdir(BLOOMBERG_RAW_DB + db + '/') if f.endswith('.pickle')]
        seclist = list(set(seclist))
        return seclist, schema_cot

#%% Database Schemas
def schema_futures(sec):
    db = 'Futures/'
    return {'px':       (BLOOMBERG_RAW_DB + db + sec + '.pickle',       'px_last'),
            'vlm':      (BLOOMBERG_RAW_DB + db + sec + '.pickle',       'volume'),
            'oi':       (BLOOMBERG_RAW_DB + db + sec + '.pickle',       'open_int')}

def schema_futgen(sec):
    db = 'Futures_gen/'
    return {'px':       (BLOOMBERG_RAW_DB + db + sec + '.px.pickle',    'px_last'),
            'ridx.pts': (BLOOMBERG_RAW_DB + db + sec + '.pickle',       'px_last'),
            'vlm':      (BLOOMBERG_RAW_DB + db + sec + '.pickle',       'volume'),
            'oi':       (BLOOMBERG_RAW_DB + db + sec + '.pickle',       'open_int')}

def schema_fx(sec):
    db = 'FX/'
    return {'spot':     (BLOOMBERG_RAW_DB + db + sec + '.spot.pickle',  'px_last'),
            'ridx.pct': (BLOOMBERG_RAW_DB + db + sec + '.ridx.pickle',  'px_last'),
            'frd1m':    (BLOOMBERG_RAW_DB + db + sec + '.1m.pickle',    'px_last'),
            'frd3m':    (BLOOMBERG_RAW_DB + db + sec + '.3m.pickle',    'px_last'),
            'frd6m':    (BLOOMBERG_RAW_DB + db + sec + '.6m.pickle',    'px_last'),
            'ppp':      (BLOOMBERG_RAW_DB + db + sec + '.pppp.pickle',  'px_last'),
            'cpp':      (BLOOMBERG_RAW_DB + db + sec + '.cppp.pickle',  'px_last') }

def schema_index(sec):
    db = 'Index/'
    return {'ridx.pct': (BLOOMBERG_RAW_DB + db + sec + '.pickle',        'px_last')}

def schema_interest_rates(sec):
    db = 'InterestRates/'
    return {'cb':       (BLOOMBERG_RAW_DB + db + sec + '.cb.pickle',      'px_last'),
            '1m':       (BLOOMBERG_RAW_DB + db + sec + '.1m.pickle',      'px_last'),
            '2y':       (BLOOMBERG_RAW_DB + db + sec + '.2y.pickle',      'px_last'),
            '7y':       (BLOOMBERG_RAW_DB + db + sec + '.7y.pickle',      'px_last'),
            '10y':      (BLOOMBERG_RAW_DB + db + sec + '.10y.pickle',     'px_last')}

def schema_cot(sec):
    db = 'CoT/'
    return {'cm.F.L':       (BLOOMBERG_RAW_DB + db + sec + '.cm.f.long.pickle', 'px_last'),
            'cm.F.S':       (BLOOMBERG_RAW_DB + db + sec + '.cm.f.short.pickle', 'px_last'),
            'cm.FO.L':      (BLOOMBERG_RAW_DB + db + sec + '.cm.fo.long.pickle', 'px_last'),
            'cm.FO.S':      (BLOOMBERG_RAW_DB + db + sec + '.cm.fo.short.pickle', 'px_last'),
            'cmb.F.L':      (BLOOMBERG_RAW_DB + db + sec + '.cmb.f.long.pickle', 'px_last'),
            'cmb.F.S':      (BLOOMBERG_RAW_DB + db + sec + '.cmb.f.short.pickle', 'px_last'),
            'cmb.FO.L':     (BLOOMBERG_RAW_DB + db + sec + '.cmb.fo.long.pickle', 'px_last'),
            'cmb.FO.S':     (BLOOMBERG_RAW_DB + db + sec + '.cmb.fo.short.pickle', 'px_last'),
            'mm.F.L':       (BLOOMBERG_RAW_DB + db + sec + '.mm.f.long.pickle', 'px_last'),
            'mm.F.S':       (BLOOMBERG_RAW_DB + db + sec + '.mm.f.short.pickle', 'px_last'),
            'mm.F.SPRD':    (BLOOMBERG_RAW_DB + db + sec + '.mm.f.sprd.pickle', 'px_last'),
            'mm.FO.L':      (BLOOMBERG_RAW_DB + db + sec + '.mm.fo.long.pickle', 'px_last'),
            'mm.FO.S':      (BLOOMBERG_RAW_DB + db + sec + '.mm.fo.short.pickle', 'px_last'),
            'mm.FO.SPRD':   (BLOOMBERG_RAW_DB + db + sec + '.mm.fo.sprd.pickle', 'px_last'),
            'nc.F.L':       (BLOOMBERG_RAW_DB + db + sec + '.nc.f.long.pickle', 'px_last'),
            'nc.F.S':       (BLOOMBERG_RAW_DB + db + sec + '.nc.f.short.pickle', 'px_last'),
            'nc.F.SPRD':    (BLOOMBERG_RAW_DB + db + sec + '.nc.f.sprd.pickle', 'px_last'),
            'nc.FO.L':      (BLOOMBERG_RAW_DB + db + sec + '.nc.fo.long.pickle', 'px_last'),
            'nc.FO.S':      (BLOOMBERG_RAW_DB + db + sec + '.nc.fo.short.pickle', 'px_last'),
            'nc.FO.SPRD':   (BLOOMBERG_RAW_DB + db + sec + '.nc.fo.sprd.pickle', 'px_last'),
            'nr.F.L':       (BLOOMBERG_RAW_DB + db + sec + '.nr.f.long.pickle', 'px_last'),
            'nr.F.S':       (BLOOMBERG_RAW_DB + db + sec + '.nr.f.short.pickle', 'px_last'),
            'nr.FO.L':      (BLOOMBERG_RAW_DB + db + sec + '.nr.fo.long.pickle', 'px_last'),
            'nr.FO.S':      (BLOOMBERG_RAW_DB + db + sec + '.nr.fo.short.pickle', 'px_last'),
            'pm.F.L':       (BLOOMBERG_RAW_DB + db + sec + '.pm.f.long.pickle', 'px_last'),
            'pm.F.S':       (BLOOMBERG_RAW_DB + db + sec + '.pm.f.short.pickle', 'px_last'),
            'pm.FO.L':      (BLOOMBERG_RAW_DB + db + sec + '.pm.fo.long.pickle', 'px_last'),
            'pm.FO.S':      (BLOOMBERG_RAW_DB + db + sec + '.pm.fo.short.pickle', 'px_last'),
            'sd.F.L':       (BLOOMBERG_RAW_DB + db + sec + '.sd.f.long.pickle', 'px_last'),
            'sd.F.S':       (BLOOMBERG_RAW_DB + db + sec + '.sd.f.short.pickle', 'px_last'),
            'sd.F.SPRD':    (BLOOMBERG_RAW_DB + db + sec + '.sd.f.sprd.pickle', 'px_last'),
            'sd.FO.L':      (BLOOMBERG_RAW_DB + db + sec + '.sd.fo.long.pickle', 'px_last'),
            'sd.FO.S':      (BLOOMBERG_RAW_DB + db + sec + '.sd.fo.short.pickle', 'px_last'),
            'sd.FO.SPRD':   (BLOOMBERG_RAW_DB + db + sec + '.sd.fo.sprd.pickle', 'px_last'),
            'xr.F.L':       (BLOOMBERG_RAW_DB + db + sec + '.xr.f.long.pickle', 'px_last'),
            'xr.F.S':       (BLOOMBERG_RAW_DB + db + sec + '.xr.f.short.pickle', 'px_last'),
            'xr.F.SPRD':    (BLOOMBERG_RAW_DB + db + sec + '.xr.f.sprd.pickle', 'px_last'),
            'xr.FO.L':      (BLOOMBERG_RAW_DB + db + sec + '.xr.fo.long.pickle', 'px_last'),
            'xr.FO.S':      (BLOOMBERG_RAW_DB + db + sec + '.xr.fo.short.pickle', 'px_last'),
            'xr.FO.SPRD':   (BLOOMBERG_RAW_DB + db + sec + '.xr.fo.sprd.pickle', 'px_last')
            }

#%% Import Data

bbgDB = {}
BLOOMBERG_RAW_DB = '/Users/maciejdragan/Google Drive/_db/bbg_raw_20191001/'

dbList = ['FX', 'Futures_gen', 'Futures', 'Index', 'InterestRates', 'CoT']
bbgDB = {}

for db in dbList:
    print('Importing {}...'.format(db))
    bbgDB[db] = import_securities(*get_db_params(db))



#%% Export Data - Timeseries

FILE_EXTENSION = '.pkl'
OUTPUT_FOLDER = '/Volumes/MM_Storage/_db/'

for dbName in dbList:
    if dbName == 'Futures':
        partition = get_partition_futures(bbgDB)
    else:
        partition = get_partition_by_sec(bbgDB, dbName)

    write_data(bbgDB[dbName], OUTPUT_FOLDER, dbName, partition)


#%% Duplicate Futures write
dbName = 'Futures'
partition = get_partition_by_sec(bbgDB, 'Futures')
write_data(bbgDB[dbName], OUTPUT_FOLDER, dbName+'_single', partition)


#%% Export Data - Metadata
dbListMeta = ['Futures_gen', 'Index', 'Futures']

for db in dbListMeta:
    compile_meta(bbgDB[db], db, write=True)


#%%
futgen = compile_meta(bbgDB['Futures_gen'], 'Futures_gen', write=False)

tckrs_first = [tckr for tckr, info in futgen.iterrows() if tckr.split('.')[0][-1]=='1']
roots = [tckr.split('.')[0][:-1] for tckr in tckrs_first]

futgen = futgen.loc[tckrs_first,:]
futgen.index = roots

futgen.to_csv(OUTPUT_FOLDER + 'Reference/futures_roots.csv')

#%%
fut    = compile_meta(bbgDB['Futures'],     'Futures',     write=False)

cols = ['LAST_TRADEABLE_DT','FUT_DLV_DT_FIRST', 'FUT_NOTICE_FIRST']
fut = fut.loc[:,cols]
fut.to_csv(OUTPUT_FOLDER + 'Reference/futures_exp_dates.csv')
