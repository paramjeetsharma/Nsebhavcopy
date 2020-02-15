# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 13:02:43 2019

@author: Paramjeet.Kumar
"""


import requests, zipfile, os, io, pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

base = 'C:/Users/paramjeet.kumar/Downloads/'
t = datetime.today().date()
dmonth={'01':'JAN','02':'FEB','03':'MAR','04':'APR','05':'MAY','06':'JUN','07':'JUL','08':'AUG','09':'SEP','10':'OCT','11':'NOV','12':'DEC'}

# Before running this script , create a file called log.txt and write the date from which you want to download EOD data
# Opening file named log.txt , which keeps track of the last downloaded date.
ltdl = open(base+'log.txt','r')
lastdt = ltdl.read(10)
ltdl.close()
lastdt = datetime.strptime(lastdt,'%Y-%m-%d')
diff, wr = t-lastdt.date(), ''
f3 = pd.DataFrame()
for i in range(1,diff.days+1):
    nextdt = lastdt+ relativedelta(days=i)
    d, m, y = '%02d' % nextdt.day, '%02d' % nextdt.month, '%02d' % nextdt.year
    if not os.path.isdir(base+y):
        os.mkdir(base+y)
        os.mkdir(base+y+'/Index')
        os.mkdir(base+y+'/Futures')
        
    zpath = base+y+'/'+d+'.zip'
    
       
    indices = requests.get('https://www1.nseindia.com/content/indices/ind_close_all_'+d+m+y+'.csv').content

    if len(indices)>300:  #sometimes nse doesnt give the index file, so the if condition
        indx = pd.read_csv(io.StringIO(indices.decode('utf-8'))) #reading content of indices csv and storing in DataFrame using io module
        indx[['Index Name', 'Index Date', 'Open Index Value', 'High Index Value', 'Low Index Value', 'Closing Index Value', 'Volume']]
        indx = indx.rename(columns={'Index Name' : 'SYMBOL', 'Index Date' : 'TIMESTAMP', 'Open Index Value' : 'OPEN', 'High Index Value' : 'HIGH', 'Low Index Value' : 'LOW', 'Closing Index Value' : 'CLOSE', 'Volume' : 'TOTTRDQTY'})
        indx.to_csv(base+y+'/Index/Indices'+ str(nextdt.date())+'.csv', index=False)
           

    header = {'Accept-Encoding': 'gzip, deflate, sdch, br',
               'Accept-Language': 'fr-FR,fr;q=0.8,en-US;q=0.6,en;q=0.4',
               'Host': 'www1.nseindia.com',
               'Referer': 'https://www1.nseindia.com/',
               'User-Agent': 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/53.0.2785.143 Chrome/53.0.2785.143 Safari/537.36',
               'X-Requested-With': 'XMLHttpRequest'
              }
    futures = requests.get('https://www1.nseindia.com/content/historical/DERIVATIVES/'+y+'/'+dmonth[m]+'/fo'+d+dmonth[m]+y+'bhav.csv.zip', headers=header)
    
   
    if futures.status_code==200:
        dload=open(zpath, 'wb')
        dload.write(futures.content)
        dload.close()
        z, wr = zipfile.ZipFile(zpath,'r'), nextdt.date()
        z.extractall(base+y+'/')
        z.close()
        os.remove(zpath)
        f0 = pd.read_csv(base+y+'/fo'+d+dmonth[m]+y+'bhav.csv') #reading the raw dl-ed bhav file
        #f0.to_csv(base+y+'/Futures'+'/Op'+str(nextdt.date())+'.csv', index=False)
        f1 = f0[f0['OPTION_TYP'] == 'XX'] #retaining only EQ rows and leaving out bonds,options etc
        f2 = f0[f0['OPTION_TYP'] != 'XX'] #retaining only EQ rows and leaving out bonds,options etc
        f1.to_csv(base+y+'/Futures'+'/Fo'+str(nextdt.date())+'.csv', index=False)
        f2.to_csv(base+y+'/Futures'+'/Op'+str(nextdt.date())+'.csv', index=False)
        os.remove(base+y+'/fo'+d+dmonth[m]+y+'bhav.csv')
   
 
if not isinstance(wr,str):
    ltdl=open(base+'log.txt','w')
    ltdl.write(str(wr))
    ltdl.close()
