# IMPORT THE SQALCHEMY LIBRARY's CREATE_ENGINE METHOD
import os
import glob
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv, dotenv_values 

# Load environment variables from a .env file if available
%reload_ext dotenv
%dotenv /Users/folder/path/ini.env

# Database credentials (recommended to set in .env)
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

CSV_FOLDER_PATH = os.getenv('CSV_FOLDER_PATH')
TABLE_NAME = 'dim_job_posts_duplicate'


# PYTHON FUNCTION TO CONNECT TO THE POSTGRESQL DATABASE AND
# RETURN THE SQLACHEMY ENGINE OBJECT
def get_connection():
	# Establish a SQLAlchemy connection to PostgreSQL DB.
	try:
		engine = create_engine( 
			url = "postgresql://{0}:{1}@{2}:{3}/{4}".format( DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME ) 
			)
		print("Database connection established.")
		return engine
	except SQLAlchemyError as e:
		print("Failed to connect to the database.")
		raise e

def list_csv_files_os(directory):
	csv_files = []
	for filename in os.listdir(directory):
		if filename.endswith(".csv") and filename.startswith("newyork_data_analyst"):
			csv_files.append(os.path.join(directory,filename))
	return csv_files


def main():
	engine = get_connection()
	conn = engine.connect()

	# Find all CSVs in the folder
	csv_files = list_csv_files_os(CSV_FOLDER_PATH) 

	if not csv_files:
		print("No CSV files found.")
		return

	for csv_file in csv_files:
		data = pd.read_csv(csv_file, encoding='unicode_escape')
		for index,row in data.iterrows():
			conn.execute("""
					INSERT INTO dim_job_posts_duplicate (job_id, scraped_date, job_title, company_name, location, seniority_lvl, employment_type, industries, time_posted_text, time_posted_numeric, time_posted_time_unit, num_applicants_text, num_applicants_numeric)
					VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
					ON CONFLICT (job_id) DO NOTHING
				""", ( row['job_id'], row['scraped_date'], row['job_title'], row['company_name'], row['location'], row['seniority_lvl'], row['employment_type'], row['industries'], row['time_posted_text'], row['time_posted_numeric'], row['time_posted_time_unit'], row['num_applicants_text'], row['num_applicants_numeric'] ) )

	conn.close()
	print("All files processed and DB connection closed.")


if __name__ == '__main__':
	main()	
