#This currently does not work. I am trying to figure out how to use dask to speed up the aggregation process.

import numpy as np
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
from scipy.stats import skew, kurtosis

def range_func(x):
    return x.map_partitions(lambda s: s.max() - s.min(), meta=('x', 'f8'))

def iqr_func(x):
    return x.map_partitions(lambda s: s.quantile(0.75) - s.quantile(0.25), meta=('x', 'f8'))

def abs_skew(x):
    return x.map_partitions(lambda s: np.abs(skew(s)), meta=('x', 'f8'))

def kurt(x):
    return x.map_partitions(lambda s: kurtosis(s), meta=('x', 'f8'))

def msa(x, y, z):
    return np.abs(np.sqrt(x**2 + y**2 + z**2) - 1)

import pandas as pd
from scipy.stats import skew, kurtosis

def custom_aggregate(group):
    results = {
        'serial' : group.name[0],
        'time' : group.name[1],
        'x_mean': group['x'].mean(),
        'x_median': group['x'].median(),
        'x_std': group['x'].std(),
        'x_min': group['x'].min(),
        'x_max': group['x'].max(),
        'x_range': group['x'].max() - group['x'].min(),
        'x_iqr': group['x'].quantile(0.75) - group['x'].quantile(0.25),
        'x_skew_abs': np.abs(skew(group['x'])),
        'x_kurtosis': kurtosis(group['x']),

        'y_mean': group['y'].mean(),
        'y_median': group['y'].median(),
        'y_std': group['y'].std(),
        'y_min': group['y'].min(),
        'y_max': group['y'].max(),
        'y_range': group['y'].max() - group['y'].min(),
        'y_iqr': group['y'].quantile(0.75) - group['y'].quantile(0.25),
        'y_skew_abs': np.abs(skew(group['y'])),
        'y_kurtosis': kurtosis(group['y']),

        'z_mean': group['z'].mean(),
        'z_median': group['z'].median(),
        'z_std': group['z'].std(),
        'z_min': group['z'].min(),
        'z_max': group['z'].max(),
        'z_range': group['z'].max() - group['z'].min(),
        'z_iqr': group['z'].quantile(0.75) - group['z'].quantile(0.25),
        'z_skew_abs': np.abs(skew(group['z'])),
        'z_kurtosis': kurtosis(group['z']),

        'msa_mean': group['msa'].mean(),
        'msa_median': group['msa'].median(),
        'msa_std': group['msa'].std(),
        'msa_min': group['msa'].min(),
        'msa_max': group['msa'].max(),
        'msa_range': group['msa'].max() - group['msa'].min(),
        'msa_iqr': group['msa'].quantile(0.75) - group['msa'].quantile(0.25),
        'msa_skew_abs': np.abs(skew(group['msa'])),
        'msa_kurtosis': kurtosis(group['msa']),
    }
    return pd.Series(results)

if __name__ == '__main__':
    
    client = Client()
    print(client.dashboard_link)

    def main():

        df = dd.read_csv("C:/Users/Andreas/Downloads/accdata_20210601_20210901_3955605.csv")


        meta={
            'serial' : 'category', 'time' : 'datetime64[ns]',
            'x_mean': 'f8', 'x_median': 'f8', 'x_std': 'f8', 'x_min': 'f8', 'x_max': 'f8', 'x_range': 'f8', 'x_iqr': 'f8', 'x_skew_abs': 'f8', 'x_kurtosis': 'f8',
            'y_mean': 'f8', 'y_median': 'f8', 'y_std': 'f8', 'y_min': 'f8', 'y_max': 'f8', 'y_range': 'f8', 'y_iqr': 'f8', 'y_skew_abs': 'f8', 'y_kurtosis': 'f8',
            'z_mean': 'f8', 'z_median': 'f8', 'z_std': 'f8', 'z_min': 'f8', 'z_max': 'f8', 'z_range': 'f8', 'z_iqr': 'f8', 'z_skew_abs': 'f8', 'z_kurtosis': 'f8',
            'msa_mean': 'f8', 'msa_median': 'f8', 'msa_std': 'f8', 'msa_min': 'f8', 'msa_max': 'f8', 'msa_range': 'f8', 'msa_iqr': 'f8', 'msa_skew_abs': 'f8', 'msa_kurtosis': 'f8'}
        
        df['time'] = dd.to_datetime(df['time'])
        df['msa'] = msa(df['x'], df['y'], df['z'])
        df['serial'] = df['serial'].astype('category')


        # Aggregating the data by 'serial' and 'time' (rounded to the nearest second)
        # This groups the data for every second for each serial
        df_grouped = df.groupby([df['serial'], df['time'].dt.round('S')], observed=False)

        aggregated_data = df_grouped.apply(custom_aggregate, meta=meta).reset_index(drop=True)
        
        #compute
        computed_data = aggregated_data.compute()

        #save to csv
        computed_data.to_csv("F:/data/accdata_aggregated_*.csv", index=False)

    main()