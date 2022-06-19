# Project Title
### Data Engineering Capstone Project

#### Project Summary
The purpose of this project is building an ETL pipeline that extracts Immigration & Airport data from S3, stages them in Redshift, and transforms data into a set of dimensional and fact tables for the analytics team in order to run analytical SQL queries and continue finding insights.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

```
import os
import re
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# set visibility
pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)<\code>
```

```
# lauch spark session
APP_NAME = 'capstone-project'

spark = (
    SparkSession
    .builder
    .appName(APP_NAME)
    .config("spark.jars.repositories", "https://repos.spark-packages.org/")
    .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")
    .enableHiveSupport()
    .getOrCreate()
)
print(f'Spark is ready\n')
spark
```
### Step 1: Scope the Project and Gather Data

#### Scope 
The purpose of this project is building an ELT pipeline that extracts Immigration, Demographics & Airport data from S3, stages them in Redshift, and transforms data into a set of dimensional and fact tables for the analytics team in order to run analytical SQL queries in efficient way, and continue finding insights. For table storage and transformation we will be using Redshift Cluster along with Postgres SQL. Moreover, all single step is monitoring and organized thanks to Airflow.

#### Describe and Gather Data 
For the scope of this project data with regards to Immigration, Demographics & Airport was used. Additionally, mapping dictionary from `I94_SAS_Labels_Descriptions.SAS` file was applied to Immigration data. Below is description of mentioned datasets:
- **Immigration** - this data comes from the US National Tourism and Trade Office ([link](https://www.trade.gov/national-travel-and-tourism-office))
- **Airport Codes** - this data contains the list of all airport codes, the attributes are identified in datapackage description. Some of the columns contain attributes identifying airport locations, other codes (IATA, local if exist) that are relevant to identification of an airport ([link](https://datahub.io/core/airport-codes#data))

The data downloaded and uploaded into S3 Bucket named `capstone-project-mt` which can be accessible via Launch Cloud Gateway with regards to fifth project (Pipeline).

```
# Read in the data here
immigration = spark.read.parquet('sas_data/part-00000-b9542815-7a8d-45fc-9c67-c9c5007ad0d4-c000.snappy.parquet').toPandas() # sample file
air_codes = pd.read_csv('airport-codes_csv.csv', sep = ',')
demo = pd.read_csv('us-cities-demographics.csv', sep = ';')
```

### Step 2: Explore and Assess the Data
#### Explore the Data 
Identify data quality issues, like missing values, duplicate data, etc.

```
def basic_data_quality_check(df: pd.DataFrame, df_name: str) -> None:
    null = pd.DataFrame(df.isna().any())
    nul_cols = null.loc[null[0] == True].index.tolist()
    duplicated_status = df.duplicated().any()
    return(
        print(f'Table {df_name} contains {df.shape[1]} columns and {df.shape[0]} rows.\nColumns list with missing values: {nul_cols}.\nDuplicated status is {duplicated_status}\n')
    )
    
for i,j in zip([immigration, air_codes, demo], ['immigration', 'air_codes', 'demo']):
    basic_data_quality_check(i, j)
```
```
immigration.head()
```
```
air_codes.head()
```
```
demo.head()
```

#### Cleaning Steps
Based on information above, rows with missing values could be either replaced with zeros or deleted. However, if we go down the road with the last option then there is high probability to omit important information. Competent person from analytics team have to decide which option is the most convenient. We will work further on data as it is.

#### Dealing with `I94_SAS_Labels_Descriptions.SAS` file
The file contains crucial information with regards to mapping certain columns in Immigration dataset. The mapping fields will be retrived from `I94_SAS_Labels_Descriptions.SAS` into several `.csv` files which will be stored in `helper_tables` folder and upload afterwards into `capstone-project-mt` S3 Bucket. Whole process is mirroring into code below:
```
# read SAS file
with open('./I94_SAS_Labels_Descriptions.SAS') as f:
    f_content = f.read()
    f_content = f_content.replace('\t', '')

## this function is taken from one of the Mentor's answer from Knowledge serction
def code_mapper(file: str, idx: str) -> dict:
    f_content2 = f_content[f_content.index(idx):]
    f_content2 = f_content2[:f_content2.index(';')].split('\n')
    f_content2 = [i.replace("'", "") for i in f_content2]
    dic = [i.split('=') for i in f_content2[1:]]
    dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
    return dic
##

# list of keys
VAR_CODE_LIST = re.findall('i94.*', f_content) + ['I94VISA']

# loop over each key
for n,i in enumerate(VAR_CODE_LIST):
    try:
        (
            pd.DataFrame(code_mapper(f_content, i), index = {0})
            .T
            .reset_index()
            .rename(columns = {'index' : 'key', 0 : f"{i.lower() if i == 'I94VISA' else i}"})
            .to_csv(f"helper_tables/{i.lower() if i == 'I94VISA' else i}.csv", index = False)
        )
        print(f'{n + 1}. {i} is processed and saved into helper_tables folder')
        globals()[i.lower() if i == 'I94VISA' else i] = pd.read_csv(f"helper_tables/{i.lower() if i == 'I94VISA' else i}.csv")
        print('and read again.\n')
    except Exception as e:
        print(e)
```

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
The Star Schema was used in order to create fact table - `immigration` - and dimension tables: 
- `passenger`
- `flights`
- `flight_flags`
- `visas`
- `airport_code`
- `country_codes`
- `airport_type`
- `states`
- `airport_city`
- `visa_type`

<img width="1500" alt="Pipeline" src="https://github.com/mkangoor/udacisty/blob/d80d2e5d54973e543429d312430f84ab4816c23b/capstone-project/ing/db-schema.png">

#### 3.2 Mapping Out Data Pipelines
The pipeline starts from copying necessary data from S3 Bucket into Redshift Tables. Afterwards, fact table is creating along with dimensional tables. Next steps are two data quality checks that will justify whether:
- number of records for all the tables are is expected
- primary key for a given table is unique one

<img width="1200" alt="Pipeline" src="https://github.com/mkangoor/udacisty/blob/d80d2e5d54973e543429d312430f84ab4816c23b/capstone-project/ing/data-pipeline.png">

Pipeline logic is shared vid Git Hub Repo ([link](lome-link))

#### 4.2 Data Quality Checks
Last two steps of presented pipeline are data quality checks that will justify whether:
- number of records for all the tables are is expected (simple count of inserted and read rows)
- primary key for a given table is unique one (group procedure by primary key with having filter)

```
DQ_COUNT_DICT = [
    {'query' : 'SELECT COUNT(*) FROM public.immigration', 'expected_outcome' : 3096313},
    {'query' : 'SELECT COUNT(*) FROM public.passenger', 'expected_outcome' : 3096313},
    {'query' : 'SELECT COUNT(*) FROM public.flights', 'expected_outcome' : 3096313},
    {'query' : 'SELECT COUNT(*) FROM public.flight_flags', 'expected_outcome' : 3096313},
    {'query' : 'SELECT COUNT(*) FROM public.visas', 'expected_outcome' : 3096313},
    {'query' : 'SELECT COUNT(*) FROM public.airport_code', 'expected_outcome' : 55075},
    {'query' : 'SELECT COUNT(*) FROM public.country_codes', 'expected_outcome' : 289},
    {'query' : 'SELECT COUNT(*) FROM public.airport_city', 'expected_outcome' : 660},
    {'query' : 'SELECT COUNT(*) FROM public.airport_type', 'expected_outcome' : 4},
    {'query' : 'SELECT COUNT(*) FROM public.states', 'expected_outcome' : 55},
    {'query' : 'SELECT COUNT(*) FROM public.visa_type', 'expected_outcome' : 3}
]

DQ_PK_UNIQUE_STATEMENT = '''SELECT {}, COUNT(*) FROM public.{} GROUP BY {} HAVING COUNT(*) > 1'''

DQ_PK_UNIQUE_DICT = [
    {'query' : DQ_PK_UNIQUE_STATEMENT.format('admission_no', 'immigration', 'admission_no'), 'expected_outcome' : 0},
    {'query' : DQ_PK_UNIQUE_STATEMENT.format('passenger_id', 'passenger', 'passenger_id'), 'expected_outcome' : 0},
    {'query' : DQ_PK_UNIQUE_STATEMENT.format('flight_id', 'flights', 'flight_id'), 'expected_outcome' : 0},
    {'query' : DQ_PK_UNIQUE_STATEMENT.format('cic_id', 'flight_flags', 'cic_id'), 'expected_outcome' : 0},
    {'query' : DQ_PK_UNIQUE_STATEMENT.format('visa_id', 'visas', 'visa_id'), 'expected_outcome' : 0},
    {'query' : DQ_PK_UNIQUE_STATEMENT.format('ident', 'airport_code', 'ident'), 'expected_outcome' : 0}
]
```
#### 4.3 Data dictionary 
Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.

#### Step 5: Complete Project Write Up
* S3 Bucket was chosen for data storing, it's quite compatible with Redshift Cluster that was utilized as a main engine of table transformation and storage. Whole the process was octhestried by Airflow that was easely connected with S3 and Redshift throught Connections created in Admin tab.
* Tables from staging table should be updated either daily, monthly or quarterly depends of data availability and purposes of analytical team. Other data such as `airport_code` or five dictionary tables should remain untouched as their values are not going to change frequently with time.
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x - **partition should be introduced**
 * The data populates a dashboard that must be updated on a daily basis by 7am every day - **set up a sensor operator in Airflow that will be monitoring data availability for a particular day and upload all available data into table either with append mode or partition**
 * The database needed to be accessed by 100+ people - **structural improvement; Redshift is powerful database itself**
