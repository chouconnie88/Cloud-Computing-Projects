#!/usr/bin/env python
# coding: utf-8

# In[17]:


#packages
import pandas as pd


# In[15]:


import pandas as pd
import gzip

# Corrected path to the gzip file
file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000036.csv.gzip'

# Open the gzip file and read it as a CSV
with gzip.open(file_path, 'rt') as file:
    threesix_df = pd.read_csv(file)

# Display the first few rows of the DataFrame
threesix_df.head()


# In[24]:


import pandas as pd
import gzip

def preprocessing(url):
    # Open the gzip file and read it as a CSV
    with gzip.open(url, 'rt') as file:
        df = pd.read_csv(file)
    
    # Change timestamp to datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors='coerce')
    
    # Systematic sampling to get every 50 rows, including the first row
    sampling_rate = 50
    sampled_df = df.iloc[::sampling_rate]

    # Return the sampled DataFrame
    return sampled_df


# In[25]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000036.csv.gzip'
threesix = preprocessing(file_path)
print(threesix.head())


# In[26]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000037.csv.gzip'
threeseven = preprocessing(file_path)
print(threeseven.head())


# In[27]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000038.csv.gzip'
threeeight = preprocessing(file_path)
print(threeeight.head())


# In[28]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000039.csv.gzip'
threenine = preprocessing(file_path)
print(threenine.head())


# In[29]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000040.csv.gzip'
forty = preprocessing(file_path)
print(forty.head())


# In[30]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000041.csv.gzip'
fortyone = preprocessing(file_path)
print(fortyone.head())


# In[31]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000042.csv.gzip'
fortytwo = preprocessing(file_path)
print(fortytwo.head())


# In[32]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000043.csv.gzip'
fortythree = preprocessing(file_path)
print(fortythree.head())


# In[33]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000044.csv.gzip'
fortyfour = preprocessing(file_path)
print(fortyfour.head())


# In[34]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000045.csv.gzip'
fortyfive = preprocessing(file_path)
print(fortyfive.head())


# In[35]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000046.csv.gzip'
fortysix = preprocessing(file_path)
print(fortysix.head())


# In[36]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000047.csv.gzip'
fortyseven = preprocessing(file_path)
print(fortyseven.head())


# In[37]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000048.csv.gzip'
fortyeight = preprocessing(file_path)
print(fortyeight.head())


# In[38]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000049.csv.gzip'
fortynine = preprocessing(file_path)
print(fortynine.head())


# In[39]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000050.csv.gzip'
fifty = preprocessing(file_path)
print(fifty.head())


# In[40]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000051.csv.gzip'
fiftyone = preprocessing(file_path)
print(fiftyone.head())


# In[41]:


file_path = r'C:\Users\mlbom\Downloads\2023_place_canvas_history-000000000052.csv.gzip'
fiftytwo = preprocessing(file_path)
print(fiftytwo.head())


# In[42]:


#concating code
combined_df = pd.concat([threesix, threeseven, threeeight, threenine, forty, fortyone, fortytwo, fortythree, fortyfour, fortyfive, fortysix, fortyseven, fortyeight, fortynine, fifty, fiftyone, fiftytwo], ignore_index=True)


# In[45]:


import pandas as pd
import numpy as np

# Assuming threesix, threeseven, etc., are already defined DataFrames
combined_df = pd.concat([threesix, threeseven, threeeight, threenine, forty, fortyone, fortytwo, fortythree, fortyfour, fortyfive, fortysix, fortyseven, fortyeight, fortynine, fifty, fiftyone, fiftytwo], ignore_index=True)

# Define the file path for the gzip-compressed CSV
gzip_file_path = r'C:\Users\mlbom\Downloads\combined_dataset.csv.gz'

# Export the combined DataFrame as a gzip-compressed CSV file
combined_df.to_csv(gzip_file_path, index=False, compression='gzip')

# Handling Large DataFrame: Splitting into chunks and saving each chunk as a separate CSV file
chunk_size = 10000  # Define the chunk size
num_chunks = len(combined_df) // chunk_size + 1  # Calculate number of chunks

# Iterate through chunks and save each as a separate CSV file
for i in range(num_chunks):
    chunk = combined_df[i*chunk_size:(i+1)*chunk_size]
    chunk_file_path = r'C:\Users\mlbom\Downloads\chunk_{}.csv'.format(i)
    chunk.to_csv(chunk_file_path, index=False)


# In[ ]:


# Define the file path where you want to save the CSV
#output_file_path = r'C:\Users\mlbom\Downloads\combined_dataset.csv'

# Export the DataFrame to a CSV file
#combined_df.to_csv(output_file_path, index=False)

