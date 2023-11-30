#importing
import pandas as pd

#load the key and data
df_key = pd.read_excel("data/accdata_key.xlsx")
df_behavior = pd.read_csv("data/subsets/W3/w3_all.csv")

#look up serials for each group in key and apply to behavior data
merged_df = pd.merge(df_behavior, df_key[['group', 'colour', 'serial']], on=['group', 'colour'], how='left')
#replace colour with serial
df_behavior['colour'] = merged_df['serial']
#rename colour to serial
df_behavior = df_behavior.rename(columns={'colour': 'serial'})

#save to csv
df_behavior.to_csv("data/subsets/W3/w3_all_serials.csv", index=False)
