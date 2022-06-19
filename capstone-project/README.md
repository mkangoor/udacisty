## Summary of the project
The purpose of this project is building an ETL pipeline that extracts Sparkify data from S3, stages them in Redshift, and transforms data into a set of dimensional and fact tables for the analytics team to continue finding insights into what songs their users are listening to.

## Database design
The Star Schema was used in order to create fact table - `songplays` - and dimension tables: `users`, `songs`, `artists`, and `time`. Graphic illustration along with field types are shown below.

<img width="802" alt="Song_ERD" src="https://user-images.githubusercontent.com/100279095/155351060-03d59720-eafe-49e0-82ff-f75fd390965f.png">

## ETL Process
Song data from `s3://udacity-dend/song_data` S3 directory were used in order to create `songs` and `artists` dimension tables as per schema. On the other hand, Log data from `'s3://udacity-dend/log_data` directory were used in order to create indeed `songplay` along with `users` & `time` as per schema.

## How to run the Python scripts
You need to set up your working directory to `/home/workspace`. Please conduct running `aws_connection`, `create_tables.py` and `etl.py` (in that order) through terminal using follwing bash command.
Example: `python aws_connection.py`


## An explanation of the files in the repository
Whole code was packed into four Python scripts: 
- `sql_queries.py` - containing SQL queries for dropping, creating, and inserting tables, 
- `aws_connection` - creating and establishing connection to AWS Redshift cluster, 
- `create_tables.py` - importing SQL queries from `sql_queries.py` as lists and runing them using appropriate functions,
- `etl.py` - containing ETL process design. 

Rest of the `.ipynb` are just notebooks, so to say, for "playground" before implementing core mentioned `.py` scripts. 
