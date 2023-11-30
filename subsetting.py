import pandas as pd

# Define the time intervals
time_intervals = {
    'group1': [(pd.to_datetime('2021-06-01 06:03:00'), pd.to_datetime('2021-06-01 10:03:00')), 
               (pd.to_datetime('2021-06-01 16:01:00'), pd.to_datetime('2021-06-01 20:01:00'))],
    'group2': [(pd.to_datetime('2021-06-09 06:04:00'), pd.to_datetime('2021-06-09 10:04:00')), 
               (pd.to_datetime('2021-06-09 16:03:00'), pd.to_datetime('2021-06-09 20:03:00'))],
    'group3': [(pd.to_datetime('2021-06-23 06:05:00'), pd.to_datetime('2021-06-23 10:05:00')), 
               (pd.to_datetime('2021-06-23 16:09:00'), pd.to_datetime('2021-06-23 20:09:00'))],
    'group4': [(pd.to_datetime('2021-06-30 06:10:00'), pd.to_datetime('2021-06-30 10:10:00')), 
               (pd.to_datetime('2021-06-30 16:08:00'), pd.to_datetime('2021-06-30 20:08:00'))],
}

# Create a DataFrame for the intervals
intervals_df = pd.DataFrame([(start, end) for group in time_intervals.values() for start, end in group], columns=['start', 'end'])

# Initialize an empty DataFrame to store filtered data
filtered_data = pd.DataFrame()

folderpath = "F:/damn/"

# Loop through each file and filter data
for i in range(0, 73):
    print("Processing file " + str(i) + "...")
    df = pd.read_csv(folderpath + "accdata_agg_" + str(i) + ".csv")
    # Convert the 'time' column to datetime
    df['time'] = pd.to_datetime(df['time'])

    # Filter data based on the time intervals
    for _, row in intervals_df.iterrows():
        filtered_df = df[(df['time'] >= row['start']) & (df['time'] <= row['end'])]
        
        # Concatenate the filtered data
        filtered_data = pd.concat([filtered_data, filtered_df], ignore_index=True)

#print the size of the filtered data and other info
print("Filtered data size: " + str(filtered_data.shape))
print(filtered_data.info())

# We can now save it to a file
filtered_data.to_csv("F:/damn/accdata_filtered.csv", index=False)