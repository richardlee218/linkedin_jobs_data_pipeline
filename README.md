# LinkedIn Job Scraper Pipeline – Airflow Orchestration & Python Automation
This project demonstrates an end-to-end data pipeline using Apache Airflow for orchestration and Python for data extraction, transformation, and loading. As a data analyst developing analytics engineering, this serves as a showcase of my ability to build and automate a data pipelines.

## Objective
Build a modular, automated pipeline to:

- Scrape job postings for “Data Analyst” roles from LinkedIn
- Parse and clean relevant job metadata (title, company, location, time posted, number of applicants, etc.)
- Save the structured output as a CSV
- Load the final dataset into a PostgreSQL database table (dim_job_posts) for downstream analysis

## Technologies & Tools
- **Apache Airflow:** Task orchestration
- **BashOperator / PythonOperator:** Airflow task types
- **Python:** Web scraping, data transformation, and scripting
- **BeautifulSoup:** HTML parsing
- **Pandas:** Data manipulation
- **SQLAlchemy:** Database connectivity
- **PostgreSQL:** Data warehouse target

## DAG Task Flow

```
get_datetime ──► process_datetime ──► save_datetime
                                         │
                                         ▼
                         run_jobpost_script (scrapes data)
                                         │
                                         ▼
                         run_upload_jobpost_script (loads data)
```

## Key Features
✅ **linkedin_dag.py**
- Airflow DAG that orchestrates all ETL tasks
- Uses BashOperator and PythonOperator to mix shell and Python execution
- Dynamic timestamping for each run to avoid file overwrite

✅ **linkedin_jobpost_script.py**
- Scrapes job postings from LinkedIn's job guest APIs
- Parses job attributes using BeautifulSoup and regex
- Handles inconsistencies in job criteria and timestamps
- Outputs cleaned CSV with normalized fields and metadata

✅ **read_csv_load_to_db.py**
- Reads output CSV
- Connects securely to PostgreSQL using SQLAlchemy
- Performs row-level inserts into dim_job_posts

## Sample Extracted Fields
- job_id
- job_title
- company_name
- location
- seniority_lvl
- employment_type
- industries
- time_posted_numeric
- time_posted_time_unit
- num_applicants_numeric
- scraped_date

## Future Improvements
-- Extend scrape to include full job descriptions and multiple page depth
-- Iterate over CSV files within storage file system for multiple file processing
-- Implement deduplication logic before database insert
-- Add Slack/email alerts within Airflow DAG and denote task success/failure
-- Replace CSV intermediary with S3 or GCS bucket


