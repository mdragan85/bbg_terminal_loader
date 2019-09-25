
from datetime import datetime
import pandas as pd

# BloombergSymbology.py
class BbgFuturesTckr():
    def __init__(self ,tckr):
        self.tckr = tckr
        self.sec = ''.join([s + ' ' for s in tckr.split(' ')[:-1]])
        self.yk = tckr.split(' ')[-1]

    @property
    def root(self):
        if self._contains_dt:
            m ,y = self._dt
            n = len(m ) +len(y)
            return self.sec[:- n -1]
        else:
            return self.sec[:-1]

    @property
    def sec_dtl(self):
        return self.sec[-1]

    @property
    def yellowkey(self):
        return self.yk

    @property
    def _contains_dt(self):
        return any([s.isdigit() for s in self.sec])

    @property
    def _dt(self):
        if self._contains_dt:
            yr = ''.join([s for s in self.sec if s.isdigit()])
            mnth = self.sec[-len(yr ) -2:-len(yr ) -1]
            return mnth, yr
        else:
            return None

    @property
    def month(self):
        month, yr = self._dt
        return month

    @property
    def year(self):
        month ,yr = self._dt
        return yr

    @property
    def year_2digit(self):
        m ,y = self._dt

        if len(y ) == 2:
            return y
        else:
            fullyr = self._single_2_full_yr(y)
            return str(fullyr)[-2:]


    @staticmethod
    def _single_2_full_yr(y):
        cur_year = datetime.today().year
        if int(y) < int(str(cur_year)[-1] ) -1:
            adder = (cur_year // 10 ) *10 + 10
        else:
            adder = (cur_year // 10 ) *10

        return adder + int(y)

class FuturesChainReference:
    """Handles Futures Chain Data. Requires *futchain.csv* to be up to date"""

    def __init__(self, fut_chain_src):
        self._fut_chain = self._load_historical_futures_chain(fut_chain_src)

    def _load_historical_futures_chain(self, srcfile):
        """load futures chain tickers stored in csv file"""
        df = pd.read_csv(srcfile)
        return {df[bbg_root][2]: list(df[bbg_root][3:]) for bbg_root in df.columns}

    @property
    def aliases(self):
        return list(self._fut_chain.keys())

    @property
    def futchains(self):
        return self._fut_chain

    def get_futures_chain(self, alias):
        """get futures chain for given alias"""

        futchain = self._fut_chain[alias]
        return [f for f in futchain if isinstance(f, str)]

class BloombergTckrService:
    """Handles Bloomberg 2 digit/1 digit year code issues. Requires *futchain*.csv reference file to be up-to-date"""
    def __init__(self, FutChainRef):

        self.full_abb, self.abb_full = self._setup_tckr_references(FutChainRef)

    def _setup_tckr_references(self, FutChainRef):
        # compute all tckr/alias pairs
        full_abb = {}
        abb_full = {}

        for root, fut_chain in FutChainRef.futchains.items():
            abb_tckr_list = [tckr for tckr in fut_chain if not pd.isnull(tckr)]
            full_tckr_list = []

            for tckr in abb_tckr_list:
                btckr = BbgFuturesTckr(tckr)
                if len(btckr.year) > 1:
                    full_tckr_list.append(btckr.tckr)
                else:
                    for try_yr in ['1', '2', '3']:
                        try_tckr = btckr.tckr.replace(btckr.year, try_yr + btckr.year)

                        if try_tckr not in abb_tckr_list:
                            break

                    full_tckr_list.append(try_tckr)

            full_abb.update(dict(zip(full_tckr_list, abb_tckr_list)))
            abb_full.update(dict(zip(abb_tckr_list, full_tckr_list)))

        return full_abb, abb_full

    def full_to_abb_tckr(self, full_tckr):
        """converts alias ticker to bloomberg ticker"""
        return self.full_abb[full_tckr]

    def abb_to_full_tckr(self, abb_tckr):
        """converts alias ticker to bloomberg ticker"""
        return self.abb_full[abb_tckr]


class FuturesAliasService:
    """Handles Bloomberg 2 digit/1 digit year code issues"""

    def __init__(self, fut_ref_src, BbgTckrSrvc):

        self._fut_ref = self._load_futures_ref(fut_ref_src)
        self._BbgTckrSrvc = BbgTckrSrvc

    def _load_futures_ref(self, fut_ref_src):
        """load futures reference"""
        futref = pd.read_csv(fut_ref_src, index_col='Alias')
        futref['Alias'] = [s for s in futref.index]
        return futref

    @property
    def futures_ref(self):
        return self._fut_ref

    def bbg_to_alias_root(self, broot, yk):
        """convert bloomberg root, yellowkey to alias root"""
        sAlias = pd.Series([s for s in self._fut_ref.index])
        ix = pd.concat([self._fut_ref.Root == broot.lower(), self._fut_ref.YellowKey == yk], axis=1).all(axis=1)
        return self._fut_ref.loc[ix, 'Alias'][0]

    def bbg_to_alias_tckr(self, bbg_tckr):
        """converts bloomberg futures ticker to alias ticker"""

        btckr = BbgFuturesTckr(self._BbgTckrSrvc.abb_to_full_tckr(bbg_tckr))
        broot, yk = btckr.root, btckr.yk
        alias_root = self.bbg_to_alias_root(broot, yk)

        return alias_root + '.' + btckr.month + btckr.year

INPUT_PATH = '../_in/'

FutChainRef = FuturesChainReference(INPUT_PATH + 'futures_historical_chain.csv')
BbgTckrService = BloombergTckrService(FutChainRef)
FutAliasRef = FuturesAliasService(INPUT_PATH + 'fut_roots.csv', BbgTckrService)

x = FutAliasRef.bbg_to_alias_tckr('CLZ15 Comdty')
print(x)