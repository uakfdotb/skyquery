import mysql.connector as mysql

def get_db():
	db = mysql.connect(
		host='localhost',
		user='skyql',
		passwd='skyql',
		database='skyql',
		autocommit=True
	)
	cursor = db.cursor()
	cursor.skyql_db = db
	return cursor
