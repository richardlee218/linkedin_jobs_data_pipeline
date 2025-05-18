# IMPORT THE SQALCHEMY LIBRARY's CREATE_ENGINE METHOD
from sqlalchemy import create_engine
import pandas as pd

# DEFINE THE DATABASE CREDENTIALS
user = '<USERNAME>'
password = '<PASSWORD>'
host = 'db.<DB_URL>.supabase.co'
port = 5432
database = 'postgres'


# PYTHON FUNCTION TO CONNECT TO THE POSTGRESQL DATABASE AND
# RETURN THE SQLACHEMY ENGINE OBJECT
def get_connection():
	return create_engine(
		url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
			user, password, host, port, database
		)
	)

if __name__ == '__main__':

	try:
		# GET THE CONNECTION OBJECT (ENGINE) FOR THE DATABASE
		engine = get_connection()
		print(
			f"Connection to the {host} for user {user} created successfully.")
	except Exception as ex:
		print("Connection could not be made due to the following error: \n", ex)

# Establish a connection
conn = engine.connect()

data = pd.read_csv(r'/to/your/path/newyork_data_analyst_20250511_2214.csv' , encoding= 'unicode_escape')

# Run the query and store the result
for index,row in data.iterrows():
    conn.execute("""
      		INSERT INTO dim_job_posts (job_id, scraped_date, job_title, company_name, location, seniority_lvl, employment_type, industries, time_posted_text, time_posted_numeric, time_posted_time_unit, num_applicants_text, num_applicants_numeric)
    		VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
    	""", ( row['job_id'], row['scraped_date'], row['job_title'], row['company_name'], row['location'], row['seniority_lvl'], row['employment_type'], row['industries'], row['time_posted_text'], row['time_posted_numeric'], row['time_posted_time_unit'], row['num_applicants_text'], row['num_applicants_numeric'] ) )


# Close the connection
conn.close()
print('\n db connection closed.')