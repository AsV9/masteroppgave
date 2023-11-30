import pandas as pd

#load dfs
label_df = pd.read_csv("data/subsets/W3/w3_all_1s.csv")
data_df = pd.read_csv("F:/damn/accdata_filtered.csv")

#join them with inner join on time

joined_df = pd.merge(data_df, label_df, on=['time','serial'], how='inner')

#drop col 'behavior' and rename 'value' to 'drinking_milk'
joined_df = joined_df.drop(columns=['behavior'])
joined_df = joined_df.rename(columns={'value':'drinking_milk'})

#save to csv (just in case)
joined_df.to_csv("data/subsets/W3/complete_labelled_for_w3.csv", index=False)
