import mysql.connector as mysql

def get_db():
	db = mysql.connect(
		host='localhost',
		user='skyquery',
		passwd='skyquery',
		database='skyquery',
		autocommit=True
	)
	cursor = db.cursor()
	cursor.skyql_db = db
	return cursor
