# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 19:55:42 2020

@author: ajo139
"""

import pandas as pd
import matplotlib.pyplot as plt # For visualization
# import seaborn as sns # For styling
# plot for entire New Zealand
NZ_plot = pd.read_csv ('M:/outputs/data/daily_NZ_pivoted.csv')
NZ_plot.shape
NZ_plot.tail(1)
NZ_plot.dtypes
df = NZ_plot.loc[NZ_plot['YEAR'] != 'YEAR'] # To filter out rows
df.shape
df.dtypes
df.head()
convert_dict = {'YEAR': int,
                'ELEMENT': str,
                'AUCKLAND AERO AWS': float,
                'CAMPBELL ISLAND AWS': float,
                'CHATHAM ISLANDS AWS': float,
                'CHRISTCHURCH INTL': float,
                'ENDERBY ISLAND AWS': float,
                'GISBORNE AERODROME': float,
                'HOKITIKA AERODROME': float,
                'INVERCARGILL AIRPOR': float,
                'KAIKOURA': float,
                'KAITAIA': float,
                'NEW PLYMOUTH AWS': float,
                'PARAPARAUMU AWS': float,
                'RAOUL ISL/KERMADEC': float,
                'TARA HILLS': float,
                'WELLINGTON AERO AWS': float                                
               } 
  
df = df.astype(convert_dict, errors = 'ignore') 
print(df.dtypes) 
# df = df.set_index('YEAR')  To set the dataframes index
# sns.set(rc={'figure.figsize':(11, 4)})

fig, axs = plt.subplots(2,2)
df.groupby(['YEAR','ELEMENT']).mean()['AUCKLAND AERO AWS'].unstack().plot(ax = axs[0, 0])
axs[0, 0].set_title('AUCKLAND AERO AWS')
df.groupby(['YEAR','ELEMENT']).mean()['CAMPBELL ISLAND AWS'].unstack().plot(ax = axs[0, 1])
axs[0, 1].set_title('CAMPBELL ISLAND AWS')
df.groupby(['YEAR','ELEMENT']).mean()['CHATHAM ISLANDS AWS'].unstack().plot(ax = axs[1, 0])
axs[1, 0].set_title('CHATHAM ISLANDS AWS')
df.groupby(['YEAR','ELEMENT']).mean()['ENDERBY ISLAND AWS'].unstack().plot(ax = axs[1, 1])
axs[1, 1].set_title('ENDERBY ISLAND AWS')
for ax in axs.flat:
    ax.set(xlabel='YEAR', ylabel='TEMPERATURE')

# Hide x labels and tick labels for top plots and y ticks for right plots.
for ax in axs.flat:
    ax.label_outer()

fig, axs = plt.subplots(2,2)
df.groupby(['YEAR','ELEMENT']).mean()['CHRISTCHURCH INTL'].unstack().plot(ax = axs[0, 0])
axs[0, 0].set_title('CHRISTCHURCH INTL')
df.groupby(['YEAR','ELEMENT']).mean()['GISBORNE AERODROME'].unstack().plot(ax = axs[0, 1])
axs[0, 1].set_title('GISBORNE AERODROME')
df.groupby(['YEAR','ELEMENT']).mean()['HOKITIKA AERODROME'].unstack().plot(ax = axs[1, 0])
axs[1, 0].set_title('HOKITIKA AERODROME')
df.groupby(['YEAR','ELEMENT']).mean()['INVERCARGILL AIRPOR'].unstack().plot(ax = axs[1, 1])
axs[1, 1].set_title('INVERCARGILL AIRPOR')
for ax in axs.flat:
    ax.set(xlabel='YEAR', ylabel='TEMPERATURE')

# Hide x labels and tick labels for top plots and y ticks for right plots.
for ax in axs.flat:
    ax.label_outer()

fig, axs = plt.subplots(2,2)
df.groupby(['YEAR','ELEMENT']).mean()['KAIKOURA'].unstack().plot(ax = axs[0, 0])
axs[0, 0].set_title('KAIKOURA')
df.groupby(['YEAR','ELEMENT']).mean()['KAITAIA'].unstack().plot(ax = axs[0, 1])
axs[0, 1].set_title('KAITAIA')
df.groupby(['YEAR','ELEMENT']).mean()['NEW PLYMOUTH AWS'].unstack().plot(ax = axs[1, 0])
axs[1, 0].set_title('NEW PLYMOUTH AWS')
df.groupby(['YEAR','ELEMENT']).mean()['PARAPARAUMU AWS'].unstack().plot(ax = axs[1, 1])
axs[1, 1].set_title('PARAPARAUMU AWS')
for ax in axs.flat:
    ax.set(xlabel='YEAR', ylabel='TEMPERATURE')

# Hide x labels and tick labels for top plots and y ticks for right plots.
for ax in axs.flat:
    ax.label_outer()

fig, axs = plt.subplots(3)
axs[0].set_title('RAOUL ISL/KERMADEC')
df.groupby(['YEAR','ELEMENT']).mean()['RAOUL ISL/KERMADEC'].unstack().plot(ax = axs[0])
axs[1].set_title('TARA HILLS')
df.groupby(['YEAR','ELEMENT']).mean()['TARA HILLS'].unstack().plot(ax = axs[1])
axs[2].set_title('WELLINGTON AERO AWS')
df.groupby(['YEAR','ELEMENT']).mean()['WELLINGTON AERO AWS'].unstack().plot(ax = axs[2])

for ax in axs.flat:
    ax.set(xlabel='YEAR', ylabel='TEMPERATURE')

# Hide x labels and tick labels for top plots and y ticks for right plots.
for ax in axs.flat:
    ax.label_outer()


# Plot for each station
NZ_avg = pd.read_csv ('M:/outputs/data/daily_NZ1.csv')
NZ_avg.shape
NZ_avg.head(2)
df1 = NZ_avg.loc[NZ_avg['ID'] != 'ID']
df1.shape
df1.dtypes
convert_dict1 = {'ID': str,
                 'YEAR': int,
                 'ELEMENT': str,
                 'VALUE': float,
                 'NAME': str,
                 'COUNTRY': str
               } 
df1 = df1.astype(convert_dict1, errors = 'ignore') 
print(df1.dtypes)
df1.head(3)

# plot
#df1 = df1.set_index('YEAR')
fig, ax = plt.subplots(figsize=(15,7))
ax.set_ylabel('Temperature');
df1.groupby(['YEAR','ELEMENT']).mean()['VALUE'].unstack().plot(ax=ax)
# Reference: https://stackoverflow.com/questions/17071871/how-to-select-rows-from-a-dataframe-based-on-column-values

