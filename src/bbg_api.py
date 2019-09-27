
import tia.bbg.datamgr as dm
from datetime import datetime
from tia.bbg import LocalTerminal

def bbg_load_ts(bbg_tckr, bbg_flds, start='1/1/1955', end='TODAY'):
    if end == 'TODAY':
        end = datetime.now().strftime("%m/%d/%Y")

    def replace_australia(flds):
        def rep(f):
            return 'fut_norm_px' if f == 'px_last' else f

        if bbg_tckr.startswith('XM') or bbg_tckr.startswith('YM'):
            return [rep(f) for f in flds]
        else:
            return flds

    def revert_fields(flds):
        def replace_norm(f):
            return 'px_last' if f == 'fut_norm_px' else f

        return [replace_norm(f) for f in flds]

    bbg_flds = replace_australia(bbg_flds)

    res = LocalTerminal.get_historical(bbg_tckr, bbg_flds, start=start, end=end)
    df = res.as_frame()[bbg_tckr]
    df.columns = revert_fields(df.columns)

    return df


def bbg_load_meta(bbg_tckr, bbg_flds):
    resp = LocalTerminal.get_reference_data(bbg_tckr, bbg_flds)
    return resp.as_frame().loc[bbg_tckr]


def get_bbg_futures_chain(bbg_root, yellow_key):
    tckr = bbg_root.upper() + 'A ' + yellow_key
    resp = LocalTerminal.get_reference_data(tckr, 'FUT_CHAIN ', {'INCLUDE_EXPIRED_CONTRACTS': 1})
    x = resp.as_map()
    return list(x.values()[0].values()[0]['Security Description'])


