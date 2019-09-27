from datetime import datetime
import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay

import pickle

from bbg_api import *


# PUT THESE TO BloombergTSLoader.py
def get_last_bdate(dt_format="%m/%d/%Y", ndays=0):
    if datetime.now().hour < 19:
        ndays = ndays - 0  # deprecated logic

    return (pd.datetime.today() - BDay(ndays)).strftime(dt_format)


class BbgSecurity():
    FLDS = ['bb_tckr', 'alias', 'local_path', 'ts_flds', 'meta_flds', 'ts', 'meta']
    FILE_EXTENSION = '.pickle'

    def __init__(self, bb_tckr, alias, local_path, ts_flds, meta_flds, ts=None, meta=None):

        self.bb_tckr = bb_tckr
        self.alias = alias
        self.local_path = local_path
        self.ts_flds = ts_flds
        self.meta_flds = meta_flds

        # -- internals -----
        self._ts = ts
        self._meta = meta

    # --- Instantiators ----------------------------------------
    @classmethod
    def from_file(cls, filename):
        try:
            with open(filename, 'rb') as f:
                d = pickle.load(f)  # for python 3-->, encoding='latin1')

            return cls(**d)
        except IOError:
            # TODO : what's a good value to return upon error
            print('...could not read file: ' + filename)

    @classmethod
    def from_dict(cls, d):
        return cls(**d)

    # --- Properties ----------------------------------------
    @property
    def ts(self):
        if not isinstance(self._ts, pd.DataFrame):
            return pd.DataFrame(columns=self.ts_flds)
        else:
            return self._ts

    @property
    def meta(self):
        if not isinstance(self._meta, pd.Series):
            return pd.Series(index=self.meta_flds)
        else:
            return self._meta

    def __len__(self):
        return len(self.ts)

    @property
    def last_datapoint(self):
        if len(self) == 0:
            return None
        else:
            return self.ts.index[-1]

    @property
    def is_expired(self):
        if 'LAST_TRADEABLE_DT' in self.meta:
            if self.last_datapoint is not None:

                last_dt_days_ago = -(self.meta['LAST_TRADEABLE_DT'] - datetime.now()).days
                last_dt_data_delta = (self.last_datapoint - self.meta['LAST_TRADEABLE_DT']).days

                # if security expired a long time ago...
                if last_dt_days_ago > 100:
                    # if series ends within 10 days of expected expiry dt
                    if np.abs(last_dt_data_delta) <= 10:
                        return True
                # if recently expired security
                else:
                    if np.abs(last_dt_data_delta) < 1:
                        return True

        return False

    @property
    def is_up_to_date(self):
        # TODO: Implement logic here
        if len(self) == 0:
            return False
        else:
            return self.last_datapoint == pd.Timestamp(get_last_bdate())

            # --- File / IO ----------------------------------------

    def to_dict(self):
        return {f: getattr(self, f) for f in self.FLDS}

    def save(self):
        fname = self.local_path + self.alias + self.FILE_EXTENSION

        print('...saving {} to local file <{}>'.format(self.bb_tckr, fname))
        with open(fname, 'wb') as f:
            pickle.dump(self.to_dict(), f)

    def load_local_data(self):
        fname = self.local_path + self.alias + self.FILE_EXTENSION

        print('...loading local file <{}> for security={}'.format(fname, self.bb_tckr))
        localObj = self.__class__.from_file(fname)

        if localObj is not None:
            self._ts, self._meta = localObj.ts, localObj.meta

    # ---- Bloomberg Load ------------------------------------
    def _bbg_load_ts(self):
        start_dt = '1/1/1960'
        end_dt = get_last_bdate()

        try:
            print('...loading timeseries for {} --> loading data from {} to {}'.format(self.bb_tckr, start_dt, end_dt))
            res = bbg_load_ts(self.bb_tckr, self.ts_flds, start=start_dt, end=end_dt)
        except:
            print('Error loading TS fields for security {} from Bloomberg'.format(self.bb_tckr))
            return

        self._ts = res

    def _bbg_load_meta(self):
        try:
            print('...loading metadata for {}'.format(self.bb_tckr))
            res = bbg_load_meta(self.bb_tckr, self.meta_flds)
        except:
            print('Error loading Meta fields for security {} from Bloomberg'.format(self.bb_tckr))
            return

        self._meta = res

    def _ts_update(self, nOverlap=5):

        ix_cut_pre = self.ts.index[-nOverlap]
        ix_cut_pst = self.ts.index[-(nOverlap - 1)]

        # load update from bloomberg
        start_dt = ix_cut_pre.strftime("%m/%d/%Y")
        end_dt = get_last_bdate()

        print('...updating timeseries for {} --> loading data from {} to {}'.format(self.bb_tckr, start_dt, end_dt))
        ts_old = self.ts.copy()
        ts_new = bbg_load_ts(self.bb_tckr, self.ts_flds, start=start_dt, end=end_dt)

        # assign to self
        self._ts = pd.concat([ts_old.loc[:ix_cut_pre, :], ts_new.loc[ix_cut_pst:, :]], axis=0).copy()

    # ---- Load Procedures ------------------------------------
    def load_from_scratch(self):
        self._bbg_load_meta()
        self._bbg_load_ts()

    def update(self):
        print('...updating security {}'.format(self.bb_tckr))

        # Load local data
        self.load_local_data()

        # if is expired
        if self.is_expired:
            print('..security {} is expired! Done!'.format(self.bb_tckr))
            # do nothing
            return

        # if is up to date
        if self.is_up_to_date:
            print('..security {} is up to date! Done!'.format(self.bb_tckr))
            # do nothing
            return

        # if doesn't exist, load from scratch
        if len(self) == 0:
            self.load_from_scratch()
            self.save()
            return

        # update timeseries & metadata...
        # --- Update Time Series -----
        all_ts_flds_in_local_data = all([True for fld in self.ts_flds if fld in self.ts.columns])

        if all_ts_flds_in_local_data:
            # update TS
            self._ts_update()
        else:
            # load from scratch
            self._bbg_load_ts()

        # --- Update Meta -----------
        all_meta_flds_in_local_data = all([True for fld in self.meta_flds if fld in self.meta.index])

        if not all_meta_flds_in_local_data:
            self._bbg_load_meta()

        # Save back to file
        self.save()

    def __repr__(self):
        return ('<BBG SECURITY: tckr=' + self.bb_tckr + ', alias=' + self.alias +
                ', local_file=' + self.local_path + '>')

