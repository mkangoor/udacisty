## Summary of the project
The purpose of this project it to load JSON files from S3 bucket into staging tables. Asterwards, the data are loading into destination tables.

## Database design
The Star Schema was used in order to create fact table - `songplay` - and dimension tables: `users`, `songs`, `artists`, and `time`.

<img width="802" alt="Song_ERD" src="https://user-images.githubusercontent.com/100279095/155351060-03d59720-eafe-49e0-82ff-f75fd390965f.png">

## ETL Process
Song data from `s3://udacity-dend/song_data` S3 directory were used in order to create `songs` and `artists` dimension tables as per schema. On the other hand, Log data from `'s3://udacity-dend/log_data` directory were used in order to create indeed `songplay` along with `users` & `time` as per schema.

## How to run the Python scripts
You need to be in `/home/workspace` working directory. Please conduct running `aws_connection`, `create_tables.py` and `etl.py` (in this order) through terminal using follwing bash command.
Example: `python aws_connection.py`


## An explanation of the files in the repository
Whole code was packed into three Python scripts: `sql_queries.py` ((containing SQL queries for dropping, creating, and inserting tables), `aws_connection` (creating and establishing connection to AWS Redshift cluster), `create_tables.py` (trigger for running sql_queries.py) and `etl.py` (containing ETL process). Rest of the `.ipynb` are just notebooks, so to say, for "playground" before implementing core mentioned `.py` scripts. 