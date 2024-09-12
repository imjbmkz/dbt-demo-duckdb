# dbt Demo Project for DuckDB
This is a demo project for creating dbt models in DuckDB.

## Prerequisites
- Clone this repository
- Install Python 3.11 from [Python.org](https://www.python.org/downloads/)
- Create a virtual environment for this project
```sh
python -m venv env # create virtual environment
```
- Activate the virtual environment
```sh
.\env\Scripts\Activate.ps1 # activate virtual environment
```
- Install dependencies 
```sh
pip install -r requirements.txt
```

## Setting-up sample data
To create sample data, run the following command.
```sh
python ingest_data.py 
```

## Running dbt project
To run the sample dbt models, run the following commands.
- Setup your `profiles.yml` in your user '.dbt' folder
```sh
dbt_demo_duckdb:
  outputs:
    dev:
      type: duckdb
      path: dev.duckdb
      threads: 1

    # optional configuration 
    # prod:
    #   type: duckdb
    #   path: prod.duckdb
    #   threads: 4

  target: dev
```

- Run the command below to run sample dbt models
```sh
cd dbt_demo_duckdb
dbt run
```