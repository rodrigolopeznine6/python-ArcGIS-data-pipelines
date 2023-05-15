'''Survey123 to SQL Server

Author: Rodrigo Lopez
Purpose: This is a script for incrementally loading transformed survey 123 data into a SQL database for analytic purposes.

Users are recording what apicultural activities they perform with they visit honey bee yards. The stakeholder would like to use the data
to measure operational effecieny and eventually, over time, analyze how the timing of a certain activity is affected by weather patterns.

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
survey_df = survey_df[survey_df['date'] > min_date]

# transform DataFrame to match destination schema
columns = ['date_col', 'attribute_col','activity_col']
new_data_df = pd.DataFrame(columns=columns)

# survey123 select multiple question stores selections as comma seperated string
# we need to create a row for every selection to optimize our data for analysis
for index, row in survey_df.iterrows():
    selection = str(row.select_multiple_col).split(',')
    for i in range(len(selection)):
        # empty dictionary
        data = {}
        data['date_col'] = row.col1
        data['attribute_col'] = row.col2
        data['activity_col'] = selection[i]
        
        #append to our new DataFrame
        new_data_df = new_data_df.append(data, ignore_index=True)
        
# Loop through transformed data and insert the rows into our destination SQL table
for index, row in new_data_df.iterrows():
    cursor.execute("INSERT INTO table_name (field_1, field_2,... field_n) VALUES (?, ?,... ?)",
                  row.field_1, row.field_2,... row.field_3)

# commit & close connection
conn.commit()
cursor.close()
    


