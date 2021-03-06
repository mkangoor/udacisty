## Summary of the project
The project is regarding to Postgres database with tables designed to optimize queries on song play analysis.

## Database design
The Star Schema was used in order to create fact table - `songplay` - and dimension tables: `users`, `songs`, `artists`, and `time`.

<img width="802" alt="Song_ERD" src="https://user-images.githubusercontent.com/100279095/155351060-03d59720-eafe-49e0-82ff-f75fd390965f.png">

## ETL Process
Song data from `/home/workspace/data/song_data/` directory were used in order to create `songs` and `artists` dimension tables as per schema. On the other hand, Log data from `/home/workspace/data/log_data/` directory were used in order to create indeed `songplay` along with `users` & `time` as per schema.

## How to run the Python scripts
You need to be in `/home/workspace` working directory. Please conduct running only `create_tables.py` and `etl.py` (in this order) through terminal using follwing bash command.
Example: `python create_tables.py`


## An explanation of the files in the repository
Whole code was packed into three Python scripts: `sql_queries.py` (containing SQL queries for dropping, creating, and inserting tables), `create_tables.py` (trigger for running sql_queries.py) and `etl.py` (containing ETL process). Rest of the `.ipynb` are just notebooks, so to say, for "playground" before implementing core mentioned `.py` scripts. Last but not least is a data folder that containing JSON files related to log and song data.
