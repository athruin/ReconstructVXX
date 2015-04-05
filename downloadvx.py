#!/usr/bin/env python
# -------------------------------------------------------------------------------
# Name:        downloadvx.py
# Purpose:     get VX futures data from CBOE, process data to a single file
#
#
# Created:     15-10-2011
# Modified:    05-04-2015 (Now works with new CBOE .csv format; minor edits.)
# Copyright:   (c) Jev Kuznetsov 2011
# Licence:     BSD
#
# -------------------------------------------------------------------------------


from urllib import urlretrieve
import os
from pandas import *
import datetime
import numpy as np


m_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']  # month codes of the futures
codes = dict(zip(m_codes, range(1, len(m_codes)+1)))

dataDir = os.path.dirname(__file__)+'/data'


def saveVixFutureData(year, month, path, forceDownload=False):
    # Get future from CBOE and save to file
    fName = "CFE_{0}{1}_VX.csv".format(m_codes[month], str(year)[-2:])
    if os.path.exists(path+'\\'+fName) or forceDownload:
        print 'File already downloaded, skipping'
        return

    urlStr = "http://cfe.cboe.com/Publish/ScheduledTask/MktData/datahouse/{0}".format(fName)
    print 'Getting: %s' % urlStr
    try:
        urlretrieve(urlStr, path+'\\'+fName)
    except Exception as e:
        print e


def buildDataTable(dataDir):
    # create single data sheet
    files = os.listdir(dataDir)

    data = {}
    for fName in files:
        print 'Processing: ', fName

        try:
            code = fName.split('.')[0].split('_')[1]
            header = 0
            with open(dataDir+'/'+fName, 'r') as fp:
                if not fp.readline().startswith('Trade Date'):
                    header = 1
                fp.close()
            df = DataFrame.from_csv(dataDir+'/'+fName, header=header)

            month = '%02d' % codes[code[0]]
            year = '20'+code[1:]
            newCode = year+'_'+month
            data[newCode] = df
        except Exception as e:
            print 'Could not process:', e

    full = DataFrame()
    for k, df in data.iteritems():
        s = df['Settle']
        s.name = k
        s[s < 5] = np.nan
        if len(s.dropna()) > 0:
            full = full.join(s, how='outer')
        else:
            print s.name, ': Empty dataset.'

    full[full<5]=np.nan
    full = full[sorted(full.columns)]

    # use only data after this date
    startDate = datetime.datetime(2008, 1, 1)

    idx = full.index >= startDate
    full = full.ix[idx, :]

    print 'Saving vix_futures.csv'
    full.to_csv('vix_futures.csv')


if __name__ == '__main__':

    if not os.path.exists(dataDir):
         print 'creating data directory %s' % dataDir
         os.makedirs(dataDir)

    for year in range(2008,2016):
        for month in range(12):
            print 'Getting data for {0}/{1}'.format(year, month+1)
            saveVixFutureData(year, month, dataDir)

    print 'Raw data was saved to {0}'.format(dataDir)

    buildDataTable(dataDir)
