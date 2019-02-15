import requests
import json
import psycopg2


try:
	print "Hello world!\nOpenning database connection...\n"
	connection = psycopg2.connect(user="skfjkefjkwe",password="sdfjskd",host="localhost",port="5432")
	cursor = connection.cursor()
	cursor.execute("SELECT * FROM ORGANIZATIONS")
	rows=cursor.fetchall()
	
	print "Fetched %s rows" % len(rows)
	for row in rows:
		print "%s\t%s\t%s\t%s\n" % (row[0],row[1],row[2],row[3])
		
	print "Exporting %s rows..\n" % len(rows)
	cursor.execute("COPY (SELECT * FROM ORGANIZATIONS) TO '/tmp/export_org.csv' WITH CSV")
except Exception, e:
	print("Failed in doing this job. %s" % e)
finally:
	if (connection is not None):
		cursor.close()
		connection.close()
		print "Connection closed\n"
		
