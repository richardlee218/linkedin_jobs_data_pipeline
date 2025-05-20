#!/usr/bin/env pyenv exec python3

import requests
from bs4 import BeautifulSoup
import random
import pandas as pd
import re
import time
import sys, os
from dotenv import load_dotenv, dotenv_values 

# Load environment variables from a .env file if available
%reload_ext dotenv
%dotenv /Users/folder/path/ini.env

CSV_FOLDER_PATH = os.getenv('CSV_FOLDER_PATH')

# Function creates a list of all numbers found within text
def extract_numbers(text):
    return re.findall(r'\d+', text)

# Function to extract the first number from a string
def extract_first_number(text):
    match = re.search(r'\d+', str(text))
    return match.group() if match else None

# Function to extract time unit
# Matches singular and plural and returns only the base units
def extract_time_unit(text):
    match = re.search(r'\b(hour|day|week|month|year)s?\b', str(text))
    return match.group(1)+'s' if match else None

today_timestamp = pd.Timestamp.today()
today_date_int = today_timestamp.strftime('%Y%m%d')
today_time_int = today_timestamp.strftime('%H%M')

title = "Data Analyst"
location = "New York"
#https://www.linkedin.com/jobs/search?keywords=Data%20Analyst&location=New%20York%2C%20New%20York%2C%20United%20States&position=1&pageNum=0

list_start = "0" # outputs 10 psots per page should increase in increments of 10 starting from 0
list_url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=Data%20Analyst&location=New%20York%2C%20New%20York%2C%20United%20States&start={list_start}"

response = requests.get(list_url)

list_data = response.text
list_soup = BeautifulSoup(list_data, "html.parser")
page_jobs = list_soup.find_all("li")

id_list = []
for job in page_jobs:
    base_card_div = job.find("div",{"class": "base-card"})
    job_id = base_card_div.get("data-entity-urn").split(":")[3]
    #print(job_id)
    id_list.append(job_id)

job_list = []

for job_id in id_list:
	#for single post testing. job_id = "4217125195" 
	job_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
	job_response = requests.get(job_url)
	#job_response.status_code 200 means successful.
	job_soup = BeautifulSoup(job_response.text, "html.parser")
	#create dict of job_post and job_criteria
	job_post = {}
	job_criteria = {}
	try:
		job_post['job_id'] = job_id
	except:
		job_post["job_id"] = None
	try:
		job_post["job_title"] = job_soup.find("h2",{"class":"top-card-layout__title font-sans text-lg papabear:text-xl font-bold leading-open text-color-text mb-0 topcard__title"}).text.strip()
	except:
		job_post["job_title"] = None
	try:
		job_post["company_name"] = job_soup.find("a",{"class":"topcard__org-name-link topcard__flavor--black-link"}).text.strip()
	except:
		job_post["company_name"] = None
	try:
		job_post["location"] = job_soup.find("span",{"class":"topcard__flavor topcard__flavor--bullet"}).text.strip()
	except:
		job_post["location"] = None
	try:
		job_post["time_posted"] = job_soup.find("span",{"class":"posted-time-ago__text posted-time-ago__text--new topcard__flavor--metadata"}).text.strip()
	except:
		job_post["time_posted"] = None
	try:
		job_post["time_posted_2"] = job_soup.find("span",{"class":"posted-time-ago__text topcard__flavor--metadata"}).text.strip()
	except:
		job_post["time_posted_2"] = None
	try:
		job_post["num_applicants"] = job_soup.find("figcaption",{"class":"num-applicants__caption"}).text.strip()
	except:
		job_post["num_applicants"] = None
	try:
		job_post["num_applicants_2"] = job_soup.find("span",{"class":"num-applicants__caption topcard__flavor--metadata topcard__flavor--bullet"}).text.strip()
	except:
		job_post["num_applicants_2"] = None
	try:
		job_post["job_description"] = job_soup.find("div",{"class":"show-more-less-html__markup show-more-less-html__markup--clamp-after-5 relative overflow-hidden"}).get_text(separator=' ').strip()
	except:
		job_post["job_description"] = None
	# Parse the job criteria items
	criteria_list = job_soup.find_all("li", class_="description__job-criteria-item")

	# Extract the headers and their corresponding values
	for item in criteria_list:
	    header = item.find("h3", class_="description__job-criteria-subheader").get_text(strip=True)
	    value = item.find("span", class_="description__job-criteria-text").get_text(strip=True)
	    job_criteria[header] = value
	merged_dict = {**job_post, **job_criteria}
	job_list.append(merged_dict)

# converts the extracted job_list into a dataframe and cleans data by merging fields together and extracting numerics from string and appends event timestamp
jobs_df = pd.DataFrame(job_list)
jobs_df['time_posted_text'] = jobs_df['time_posted'].fillna(jobs_df['time_posted_2'])
jobs_df['num_applicants_text'] = jobs_df['num_applicants'].fillna(jobs_df['num_applicants_2'])
jobs_df['num_applicants_numeric'] = jobs_df['num_applicants_text'].apply(extract_first_number)
jobs_df['time_posted_numeric'] = jobs_df['time_posted_text'].apply(extract_first_number)
jobs_df['time_posted_time_unit'] = jobs_df['time_posted_text'].apply(extract_time_unit)
jobs_df['scraped_date'] =  pd.Timestamp.today().date()
jobs_df = jobs_df.rename(columns={"Seniority level":'seniority_lvl',"Employment type":'employment_type','Job function':'job_function','Industries':'industries'})
jobs_df_clean = jobs_df[['job_id','scraped_date','job_title','company_name','location','seniority_lvl','employment_type','job_function','industries','time_posted_text','time_posted_numeric','time_posted_time_unit','num_applicants_text','num_applicants_numeric','job_description']]

# Export the data as a CSV file to specified folder location
csv_filename = f"newyork_data_analyst_{today_date_int}_{today_time_int}.csv"
output_file = os.path.join(CSV_FOLDER_PATH, csv_filename)
jobs_df_clean.to_csv(output_file, index=False)
print(f"CSV file exported to: {output_file}")

time.sleep(2)
