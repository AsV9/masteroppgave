import pandas as pd
import numpy as np
from scipy.stats import skew, kurtosis
from datetime import datetime

# WARNING: This will easily take 13-14 hours to run

# Defining the functions to calculate the required statistics
def range_func(x):
     return x.max() - x.min()
def iqr_func(x):
    return x.quantile(0.75) - x.quantile(0.25)
def msa(x, y, z):
     return np.abs(np.sqrt(x**2 + y**2 + z**2) - 1)

chunksize = 10 ** 7
chunknum = 0
start_chunk = 16
end_chunk = 30
for chunk in pd.read_csv("C:/Users/Andreas/Downloads/accdata_20210601_20210901_3955605.csv", chunksize=chunksize):
    if(chunknum < start_chunk):
        print("Skipped chunk " + str(chunknum))
        chunknum += 1
        continue
    if(chunknum > end_chunk and end_chunk != 0):
        print("Stopping at chunk " + str(chunknum-1))
        break    
    df = chunk


    df['time'] = pd.to_datetime(df['time'])
    df['msa'] = msa(df['x'], df['y'], df['z'])
    df['serial'] = df['serial'].astype('category')

    # Aggregating the data by 'serial' and 'time' (rounded to the nearest second)
    # This groups the data for every second for each serial
    df_grouped = df.groupby([df['serial'], df['time'].dt.round('S')], observed=False)

    aggregated_data = df_grouped.agg({
        'x': ['mean', 'median', 'std', 'min', 'max', range_func, iqr_func, lambda x: np.abs(x.skew()), lambda x: x.kurt()],
        'y': ['mean', 'median', 'std', 'min', 'max', range_func, iqr_func, lambda x: np.abs(x.skew()), lambda x: x.kurt()],
        'z': ['mean', 'median', 'std', 'min', 'max', range_func, iqr_func, lambda x: np.abs(x.skew()), lambda x: x.kurt()],
        'msa': ['mean', 'median', 'std', 'min', 'max', range_func, iqr_func, lambda x: np.abs(x.skew()), lambda x: x.kurt()]
    }).reset_index()
    # Renaming the columns for clarity
    aggregated_data.columns = ['serial', 'time', 'x_mean', 'x_median', 'x_std', 'x_min', 'x_max', 'x_range', 'x_iqr', 'x_skew_abs', 'x_kurtosis',
                            'y_mean', 'y_median', 'y_std', 'y_min', 'y_max', 'y_range', 'y_iqr', 'y_skew_abs', 'y_kurtosis',
                            'z_mean', 'z_median', 'z_std', 'z_min', 'z_max', 'z_range', 'z_iqr', 'z_skew_abs', 'z_kurtosis',
                            'msa_mean', 'msa_median', 'msa_std', 'msa_min', 'msa_max', 'msa_range', 'msa_iqr', 'msa_skew_abs', 'msa_kurtosis']


    print("Saved chunk " + str(chunknum) + " at " + str(datetime.now()))
    aggregated_data.to_csv("F:/data/accdata_agg_" + str(chunknum) + ".csv", index=False)
    chunknum += 1