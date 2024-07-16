# import pandas as pd
# df = pd.read_csv('./realtimescenarios/data/test4/laptopPrice.csv')
# rows_per_file = 100
# num_files = len(df) // rows_per_file
# for i in range(num_files):
#     start_row = i * rows_per_file
#     end_row = (i + 1) * rows_per_file
#     chunk = df.iloc[start_row:end_row]
#     chunk.to_csv(f'laptopPrice_day_{i+1}.csv', index=False)

# if len(df) % rows_per_file != 0:
#     start_row = num_files * rows_per_file
#     chunk = df.iloc[start_row:]
#     chunk.to_csv(f'laptopPrice_day_{num_files+1}.csv', index=False)

import pandas as pd

csv_files = ['laptopPrice_day_5.csv','laptopPrice_day_6.csv','laptopPrice_day_7.csv','laptopPrice_day_8.csv','laptopPrice_day_9.csv']

combined_df = pd.concat((pd.read_csv(file) for file in csv_files), ignore_index=True)

combined_df.to_csv('laptopPrice_day_5_combined.csv', index=False)