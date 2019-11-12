# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 00:25:37 2019

@author: param
"""




import pandas as pd
import numpy as np


df=pd.read_csv("C:/Users/param/Downloads/NIFTY501MIN.csv")
df.columns
#data_close = df['close'].resample('15Min').ohlc()
df.drop("Unnamed: 0",axis=1,inplace=True)
df['date'] =  pd.to_datetime(df['date'])
df.set_index('date', inplace=True)

ohlc_dict = {'open':'first', 'high':'max', 'low':'min', 'close': 'last'}

df30=df.resample('30T', how=ohlc_dict, closed='left', label='left',base=15).dropna(how='any')
df1H=df.resample('60Min', how=ohlc_dict, closed='left', label='left',base=15).dropna(how='any')



def EMA(df, base, target, period, alpha=False):
    """
    Function to compute Exponential Moving Average (EMA)
    
    Args :
        df : Pandas DataFrame which contains ['date', 'open', 'high', 'low', 'close', 'volume'] columns
        base : String indicating the column name from which the EMA needs to be computed from
        target : String indicates the column name to which the computed data needs to be stored
        period : Integer indicates the period of computation in terms of number of candles
        alpha : Boolean if True indicates to use the formula for computing EMA using alpha (default is False)
        
    Returns :
        df : Pandas DataFrame with new column added with name 'target'
    """

    con = pd.concat([df[:period][base].rolling(window=period).mean(), df[period:][base]])
    
    if (alpha == True):
        # (1 - alpha) * previous_val + alpha * current_val where alpha = 1 / period
        df[target] = con.ewm(alpha=1 / period, adjust=False).mean()
    else:
        # ((current_val - previous_val) * coeff) + previous_val where coeff = 2 / (period + 1)
        df[target] = con.ewm(span=period, adjust=False).mean()
    
    df[target].fillna(0, inplace=True)
    return df




df30.reset_index(inplace=True)
df1H.reset_index(inplace=True)

tmp = pd.DataFrame()

for i in range(121,2,-1):
    for j in range (i-1,1,-1):
        df30_L=pd.DataFrame.copy(df30)
        df1H_L=df=pd.DataFrame.copy(df1H)
        EMA(df30_L, 'close', 'ema_'+ str(i), i)
        EMA(df30_L, 'close', 'ema_'+ str(j), j)
        df30_L['ema_'+ str(j)+'_Lag'] = df30_L['ema_'+ str(j)].shift(1)
        df30_L['ema_'+ str(i)+'_Lag'] = df30_L['ema_'+ str(i)].shift(1)
        df30_L['Buy'] = df30_L.apply(lambda x : 1 if x['ema_'+ str(j)] > x['ema_'+ str(i)] and x['ema_'+ str(j)+'_Lag'] < x['ema_'+ str(i)+'_Lag'] else 0, axis=1)
        df30_L['Sell'] = df30_L.apply(lambda y : -1 if y['ema_'+ str(j)] < y['ema_'+ str(i)] and y['ema_'+ str(j)+'_Lag'] > y['ema_'+ str(i)+'_Lag'] else 0, axis=1)
        df30_L['Signal']=df30_L['Buy']+df30_L['Sell']
        df30_L = df30_L.iloc[i:]
        EMA(df1H_L, 'close', 'ema_'+ str(i), i)
        EMA(df1H_L, 'close', 'ema_'+ str(j), j)
        df1H_L['ema_'+ str(j)+'_Lag'] = df1H_L['ema_'+ str(j)].shift(1)
        df1H_L['ema_'+ str(i)+'_Lag'] = df1H_L['ema_'+ str(i)].shift(1)
        df1H_L['Buy'] = df1H_L.apply(lambda x : 1 if x['ema_'+ str(j)] > x['ema_'+ str(i)] and x['ema_'+ str(j)+'_Lag'] < x['ema_'+ str(i)+'_Lag'] else 0, axis=1)
        #print (df1H_L['ema_'+ str(i)])
        df1H_L['Sell'] = df1H_L.apply(lambda y : -1 if y['ema_'+ str(j)] < y['ema_'+ str(i)] and y['ema_'+ str(j)+'_Lag'] > y['ema_'+ str(i)+'_Lag'] else 0, axis=1)
        df1H_L['Signal']=df1H_L['Buy']+df1H_L['Sell']
        df30_L = df30_L.iloc[i:]
        df = pd.merge(df30_L, df1H_L, on='date', how='outer')
        df=df.fillna(0)
        df['date']= pd.to_datetime(df['date'])
        df.sort_values("date", axis = 0, ascending = True, inplace = True, na_position ='last')
        flag=1
        df['Signal']=0
        df['Diff']=0
        Sig=0
        location=0
        df['trade']=0
        df['Trdcnt']=0
        for k in range(df.shape[0]):
            if flag==1:
                #print (flag)
                if df.at[k,'Signal_y']==0:
                    df.at[k,'Signal']=0
                else:
                    
                    
                    if df.at[k,'Signal_y']!=0:
                        flag=0
                        location=k
                        Sig=df.at[k,'Signal_y']
                        df.at[k,'Signal']=Sig
                        df.at[k,'date_cols']=df.at[k,'date']
                        df.at[k,'trade']=df.at[k,'close_y']
            elif flag==0:
                #print (k)
                if df.at[k,'Signal_x']==0:
                    df.at[k,'Signal']=0
                elif df.at[k,'Signal_x']==-Sig:
                    df.at[k,'Signal']=-Sig
                    flag=1
                    df.at[k,'Diff']=(df.at[k,'close_x']-df.at[location,'close_x'])*Sig
                    df.at[k,'trade']=df.at[k,'close_x']
                    df.at[k,'trDay']=(df.at[k,'date']-df.at[location,'date']).days
                    df.at[k,'Trdcnt']=1
                    df.at[k,'Positive']=np.where(df.at[k,'Diff']>0,1,0)
                    df.at[k,'Negative']=np.where(df.at[k,'Diff']<0,1,0)
        df['year'] = df['date'].dt.year
        #print (i,"and ", j)
        #print(i,"and ", j ,'\n',pd.pivot_table(df,index=["year"],aggfunc={'Trdcnt':np.sum,'Diff':[np.sum,np.max,np.min]}))
        Kf=pd.pivot_table(df,index=["year"],aggfunc={'Trdcnt':np.sum,'Diff':[np.sum,np.max,np.min],'Positive':np.sum,'Negative':np.sum})
        Kf['AVG']=(str(i)+" and "+str(j))
        tmp = tmp.append(Kf)
tmp.to_csv('C:/Users/param/Downloads/result.csv')
