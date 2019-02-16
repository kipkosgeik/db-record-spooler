import psycopg2
import os
import ConfigParser
import subprocess

try:
	print "Performing export data!\nOpenning database connection...\n"
	config = ConfigParser.ConfigParser()
	config.read("conf/application.properties")
	report_dir = config.get("APP", "report.directory")

	connection = psycopg2.connect(database=config.get("DB", "db.name"), user=config.get("DB", "db.username"), password=config.get("DB", "db.password"), host=config.get("DB", "db.host"), port=config.get("DB", "db.port"))
	
	cursor = connection.cursor()
	cursor.execute("SELECT * FROM bulksmstasks WHERE transactionstatus=5 AND age(current_timestamp, datecreated) > interval '5 hours' AND  age(current_timestamp, datecreated) < interval '24 hours'")
	rows = cursor.fetchall()
	
	print "Exporting bulk reports for %s task(s)..." % len(rows)
	print("ID\tUnits\tCREATED\tMODIFIED\tTotal ADDR\tSTATUS")
	for row in rows:
		task_id = row[0]
		target_filename = "%s.csv" % task_id
		print "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (row[0], row[1], row[2], row[3], row[5], row[6], row[4])
		query = ''' SELECT t.message_id AS "MESSAGE ID" ,t.recipientaddress as "MSISDN", 
			CASE t.deliverystatus 
				WHEN 4 THEN 'DeliveredToTerminal' 
				WHEN 3 THEN 'MessageWaiting' 
				WHEN 2 THEN 'DeliveryImpossible' 
				ELSE 'DeliveryUncertain' 
				END AS "DLR",
			creditunits AS "UNITS", t.datecreated AS "CREATED", t.datemodified AS "UPDATED", t.message AS "MESSAGE",
			CASE x.transactionstatus 
				WHEN 0 THEN 'CREATED' 
				WHEN 1 THEN 'PROCESSING' 
				WHEN 2 THEN 'PAUSED' 
				WHEN 3 THEN 'CANCELLED' 
				WHEN 4 THEN 'COMPLETE' 
				WHEN 5 THEN 'COMPLETE' 
				WHEN 6 THEN 'EXPIRED' 
				WHEN 7 THEN 'QUEUED' 
				WHEN 8 THEN 'INSUFFICIENT_CREDIT' 
				WHEN 9 THEN 'SCHEDULED' 
				ELSE 'UKNOWN' END as "TRANSACTION STATUS",
			requestidentifier AS "MNO Identifier",
			p.alphanumeric AS "SID",
			o.orgname AS "ORGANIZATION",
			u.email AS "USER"
			FROM textmessages t 
			INNER JOIN messagetransactions x ON t.messagetransaction_tx_id = x.tx_id 
			JOIN prsservice p ON x.prsservice_id=p.id  
			JOIN organizations o ON x.organization_org_id=o.org_id
			LEFT JOIN mobitextuser u ON u.user_id=x.mobitextuserid
				WHERE x.bulksmstask_task_id = %s ORDER BY t.datecreated DESC''' % task_id
		
		outC = subprocess.Popen(['echo', 'COPY (%s) TO STDOUT WITH CSV HEADER' % query], stdout=subprocess.PIPE)
		outP = subprocess.Popen(['psql', '-U', config.get("DB", "db.username"), '-h', config.get("DB", "db.host"), config.get("DB", "db.name"), '-o' , '%s/%s.csv' % (report_dir, task_id)], stdin=outC.stdout, stdout=subprocess.PIPE)
		stdout, stderr = outP.communicate()
		success = None == stderr
		
		# print("Copy command executed with return: %s for task ID %s" % (status, task_id))

		if (success):
			print "COPY successful. %s Zipping file %s.csv ..." % (stdout, task_id)
			os.chdir(report_dir)
			status = os.system("zip -v %s.zip %s.csv && rm -v %s.csv" % (task_id, task_id, task_id))
			# Update row to mark export as done [Errno 9] Bad file descriptor
			results = cursor.execute("UPDATE bulksmstasks SET report_file='%s/%s.zip' WHERE task_id=%s" % (report_dir, task_id, task_id))
			connection.commit()
			print("%s Affected rows on UPDATE " % results)
		else:
			print "COPY failed. %s"	 % stderr
except Exception, e:
	print("Failed in Exporting report. %s" % e)
finally:
	if (connection is not None):
		cursor.close()
		connection.close()
		print "Connection closed\n"
		
