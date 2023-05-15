'''Survey123 to SQL Server

Author: Rodrigo Lopez
Purpose: This is a simple script for downloading Survey123 data to SQL.

Notes: An improvement of this would be to filter the survey data by
a ID field such as a globalid, where we would keep only records
which have not been inserted into our destination SQL table.

'''


# Import libraries
import pyodbc
import pandas as pd
import arcgis
from arcgis.gis import GIS
from datetime import datetime as dt

# Connect to AGOL with survey owner or admin credentials
gis = GIS('https://www.arcgis.com', 'user', 'pw')

# Use the ArcGIS Survey123 module to download our survey data to a DataFrame
survey_manager = arcgis.apps.survey123.SurveyManager(gis)
survey_by_id = survey_manager.get('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
survey_df = survey_by_id.download('DF')

# Format the DataFrame date column to match the format of our destination table date column
survey_df["date"] = pd.to_datetime(survey_df["date"].dt.strftime('%Y-%m-%d'))

# Use pyodbc to establish a database connection
# In this example our destination is MS SQL Server
# I have set trusted connection param to yes, alternatively provide UID and PWD params
conn = pyodbc.connect('Driver={SQL Server};Server=SERVER;Database=DB;Trusted_Connection=yes')
cursor = conn.cursor()

# Use the cursor.execute() function to execute a SQL statement that returns the current max date in our SQL table
# Use cursor.fetchone() to obtain result
cursor.execute('SELECT MAX(date_field) FROM table_name;')
result = cursor.fetchone()

# Format the date from our SQL statement to match the format of the date field in our DataFrame date field
min_date = dt.strptime(result[0],'%Y-%m-%d')
min_date = min_date.strftime('%Y-%m-%d')

# Filter our DataFrame using our date returned by our SQL statement
# Create new DataFrame containing only data we are interested in
new_data_df = survey_df[survey_df['date'] > min_date]

# Perform any necessary transforms to new DataFrame here

# Loop through transformed dataframe using DataFrame iterrows() method
# Use cursor.execute() function to insert new rows to our table
for index, row in new_data_df.iterrows():
    cursor.execute("INSERT INTO table_name (field_1, field_2,... field_n) VALUES (?, ?,... ?)",
                  row.field_1, row.field_2,... row.field_3)

# commit & close connection
conn.commit()
cursor.close()
    


