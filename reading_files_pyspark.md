# Ways of Reading files in Python/PySpark: A Guide
*Nabin Ghimire  [LinkedIn](https://www.linkedin.com/in/nabin-ghimire-69a76124b/)

## Way 1: Read all files individually
### First, read all files individually
```python
df1 = spark.read.text("/mnt/d/Data/mp_2004.txt")
df2 = spark.read.text("/mnt/d/Data/mp_2005.txt")
# add more files as you like
dfn = spark.read.text("/mnt/d/Data/mp_n.txt") # replace mp_n with your exact file name
```
### Second, create a list or a dictionary of dataframes
```python
 data_frames = [df1, df2, ..., dfn]  # this is list
 dict_dataframes = {
    "mp_2004": df1,
    "mp_2005": df2,
    # add more file_years as needed
    "mp_nth year": dfn
}
```
### Third, access each dataframe by it's index 
```python
data_frames = [df1, df2, ..., dfn]  # this is list
data_frames[0] = df1
data_frames[1] = df2
# you can access any of the dataframes by it's index
data_frames[n-1] = dfn
```
### You can use all the methods of indexing, slicing and dicing here...
```python
# see an example to show the first few rows of all dataframes at once
# this code will iterate through all dataframes in list 'data_frames' and print the first few rows of all dataframes
for df in data_frames:
    df.show()
```

## Way 2: Read all the files at once
### First, read all files using dictionary method
```python
# here we are reading all text files from directory "/mnt/d/Data" with their respective file names \
# (which can be denoted in a series mp_2003.txt, mp_2004.txt, # add more dataframes as needed, mp_2023.txt)

# Specify the path for the files to read
file_paths = [f"/mnt/d/Data/mp_{year}.txt" for year in range(2003, 2024)] 
dict_dataframes = {}  # An empty dictionary
for year, file_path in zip(range(2003, 2024), file_paths):
    dict_dataframes[f"mp_{year}"] = spark.read.text(file_path)
```
### Second, now you can access dataframes like this
```python
# keys in the dict_dataframes corresponds to the file names respectively \ 
# such as "mp_2003", "mp_2004", # add others, "mp_2023"

dict_dataframes["mp_2003"].show()
# Or try something else like
dict_dataframes["mp_2015"].count()
# Try other functions as you wish

```
### ALTERNATIVELY
### Read all files using list method
```python
file_paths = [f"/mnt/d/Data/mp_{year}.txt" for year in range(2004, 2024)]

dataframes = []
for file_path in file_paths:
    dataframes.append(spark.read.text(file_path))

# Now you can access each DataFrame using index like \
data_frames[0], data_frames[1], ..., data_frames[19] for the years 2004 to 2023
```

#### Do you need some more sophistication? Scroll down, then!
#### Using Wildcard Method could assist further. Pyspark's 'spark.read.text' command supports wildcards too. 
```python
# 1. Single DataFrame Method 
## specify the file path and pattern
file_pattern = "/mnt/d/Data/mp_*.txt"
## read all files matching the given pattern in single dataframe
df_all = spark.read.text(file_pattern)
## now you can use other methods here, such as:
df_all.show()
df_all.count()\n

# 2. Separate DataFrame Method
import glob
# specify pattern
file_pattern = "/mnt/d/Data/mp_*.txt"
# get a list of all file paths
file_paths = glob.glob(file_pattern)
# Read DataFrames separately and in respective order
data_frames = [spark.read.text(file_path) for file_path in file_paths]
# Use indexes to access each DataFrame separately
data_frames[0].show()
data_frames[1].show()
"""
and so on
"""
```

### Conclusion
We explored various ways of reading '.txt' files. 
Yes, in addition to the methods we've mentioned, there are a few other approaches for reading files in PySpark:

1. Read CSV Files: If your data is in CSV format, you can use spark.read.csv() to directly read CSV files into a DataFrame.

2. Read Parquet Files: Parquet is a columnar storage format that is highly optimized for reading with PySpark. You can use spark.read.parquet() to read Parquet files.

3. Read JSON Files: If your data is in JSON format, you can use spark.read.json() to read JSON files into a DataFrame.

4. Read from JDBC: PySpark supports reading data from databases using JDBC. You can use spark.read.jdbc() to read data from a JDBC source.

5. Read from Hive: If you have data stored in Hive tables, you can use spark.read.table() to read the data into a DataFrame.

These are some additional methods available in PySpark for reading different types of data sources. Depending on the format and location of your data, you can choose the appropriate method for reading files.

### Please have a look at more deep Spark Application
``` python
from pyspark.sql import SparkSession
import glob

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# Define the file pattern
file_pattern = "/mnt/d/Data/mp_*.txt"

# Get the file paths
file_paths = glob.glob(file_pattern)

# Read data frames from each file
data_frames = [spark.read.text(file_path) for file_path in file_paths]

# Perform word count on each data frame and save the output
for i, df in enumerate(data_frames):
    # Convert DataFrame to RDD and apply word count
    word_counts = df.rdd.flatMap(lambda row: row.value.split()) \
                        .map(lambda word: (word, 1)) \
                        .reduceByKey(lambda a, b: a + b)
    
    # Convert RDD back to DataFrame
    word_counts_df = spark.createDataFrame(word_counts, ["Word", "Count"])
    
    # Save the word counts to a file
    word_counts_df.write.csv(f"word_counts_{i}", mode='overwrite', header=True)

# Stop SparkSession
spark.stop()
```
 ### And one more, 
 ```python
 from pyspark.sql import SparkSession
import glob

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# Define the file pattern
file_pattern = "/mnt/d/Data/mp_*.txt"

# Get the file paths
file_paths = glob.glob(file_pattern)

# Create a dictionary to store data frames
data_frames = {}

# Read data frames from each file and store them in the dictionary
for file_path in file_paths:
    df = spark.read.text(file_path)
    # Extract the index from the file name
    idx = int(file_path.split('_')[-1].split('.')[0])
    data_frames[idx] = df

# Perform word count on each data frame and save the output
for idx, df in data_frames.items():
    # Convert DataFrame to RDD and apply word count
    word_counts = df.rdd.flatMap(lambda row: row.value.split()) \
                        .map(lambda word: (word, 1)) \
                        .reduceByKey(lambda a, b: a + b)
    
    # Convert RDD back to DataFrame
    word_counts_df = spark.createDataFrame(word_counts, ["Word", "Count"])
    
    # Save the word counts to a file
    word_counts_df.write.csv(f"word_counts_{idx}", mode='overwrite', header=True)

# Stop SparkSession
spark.stop()
```

There are actually numerous ways to perform a same task. The particular approach should suffice your demands for analysis job. So choosing a great way as per the requirements of your research is crucial which involves the use of your domain specific knowledge.

Happy Sparking!

Should you have any queries or collaboration,
Join with me in [LinkedIn](https://www.linkedin.com/in/nabin-ghimire-69a76124b/)

Thank You!