
#%%
import pandas as pd

import os
import pickle

src_folder = '/Volumes/MM_Storage/CM Data/PyDB/Futures/kibot_15min/'
dest_folder = '/Volumes/MM_Storage/_db/Futures_Kibot15m/'


def decompose_tckr(oldTckr):
    root = ''.join([s for s in oldTckr if s.islower()])
    dtl = ''.join([s for s in oldTckr if s.isupper() or s.isnumeric()])

    return root, dtl


def copy_file(oldTckr):
    EXTENSION = '.pkl'

    root, dtl = decompose_tckr(oldTckr)
    newTckr = root + '.' + dtl

    srcfile = src_folder + oldTckr
    destfile = dest_folder + newTckr + EXTENSION

    print('reading {}'.format(srcfile))
    with open(srcfile, 'rb') as f:
        x = pickle.load(f)

    # new format
    y = {newTckr: x}

    print('...writing {}'.format(destfile))
    with open(destfile, 'wb+') as f:
        pickle.dump(y, f)



EXCLUSIONS = ['.DS_Store']

# main loop
for fname in os.listdir(src_folder):
    if fname not in EXCLUSIONS:
        copy_file(fname)





"""
This was used to check out some file formats
#%%
src = '/Volumes/MM_Storage/_db/Futures_gen/ad1.bbd.pkl'

with open(src, 'rb') as f:
    x = pickle.load(f)

print(x)

#%%

"""