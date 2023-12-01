'''
Incremental loading of Survey123 data to SQL Server

Author: Rodrigo Lopez
Purpose: This is a function for incrementally loading transformed survey 123 data into a SQL database for analytic purposes.

Users are recording what apicultural activities they perform with they visit honey bee yards. The stakeholder would like to use the data
to measure operational effecieny and eventually, over time, analyze how the timing of a certain activity affects honey production.

'''


def incremental_survey123_to_sql(agol_user, agol_pw, survey_id, survey_date_field,sql_conn_string,sql_table, sql_date_field):
	'''
	Arguments
		agol_user: str, username for connecting to agol
		agol_pw: str, pw for connecting to agol
		survey_id: str, the id of survey123 form being used to collect data
		survey_date_field: str, the date field of survey
		sql_conn_string: str, pyodbc connection string for connecting to sql
		sql_table: str, target sql table
		sql_date_field: str, target sql table col name
	Returns:
		None
	'''

	# agol connection
	gis = GIS('https://www.arcgis.com', agol_user, agol_pw)
	
	# survey manager
	survey_manager = arcgis.apps.survey123.SurveyManager(gis)
	survey_by_id = survey_manager.get(survey_id)
	# download survey data as df
	survey_df = survey_by_id.download('DF')

	# format survey date column to match sql date col
	survey_df[survey_date_field] = pd.to_datetime(survey_df[survey_date_field].dt.strftime('%Y-%m-%d'))

	# pyodbc connection
	conn = pyodbc.connect(sql_conn_string)
    cursor = conn.cursor()

	# max date from sql date column that will serve as min date for getting relevant data
	max_date_select_str = f'SELECT MAX({sql_date_field}) FROM {sql_table}'
	cursor.execute(max_date_select_str)

	max_sql_date = cursor.fetchone()
	min_survey_date = dt.strptime(max_sql_date[0],'%Y-%m-%d')
	min_survey_date = min_date.strftime('%Y-%m-%d')

	# may need to exclude today, depending on when running script
	todays_date = dt.today()
	todays_date = todays_date.strftime('%Y-%m-%d')

	# filter df for relevant data
	survey_df = survey_df[(survey_df[survey_date_field] > min_survey_date) & (survey_df[survey_date_field] < todays_date)]

	# transform data (optional)
	new_columns = ['date_col', 'attribute_col', 'numerical_col', 'activity_col']
	new_data_df = pd.DataFrame(columns=columns)

	for index, row in survey_df.iterrows():
		selection = str(row.select_multiple_col).split(',')
		for i in range(len(selection)):
			# empty dictionary
            data = {}
            data['date_col'] = row.col1
            data['attribute_col'] = row.col2
            data['numerical_col'] = row.col3
            data['activity_col'] = selection[i]
        
            #append to our new DataFrame
            new_data_df = new_data_df.append(data, ignore_index=True)
        
    # prepare insert statement for new data df loop
	insert_cols = ','.join(new_data_df.columns)
	insert_placeholders = ','.join(['?' for _ in new_data_df.columns])
	insert_stmt = f'INSERT INTO {sql_table} ({insert_cols}) VALUES ({insert_placeholders})'

	# loop and insert new data
	for index, row in new_data_df.iterrows():
		
		# deal with NaN values
		row = row.where(pd.notnull(row), None)
		values = row.tolist()
		
		cursor.execute(insert_stmt, values)

	# commit and close connection        
    conn.commit()
	conn.close()
    


