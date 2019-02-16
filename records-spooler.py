import psycopg2
import os
import ConfigParser

try:
	print "Performing export data!\nOpenning database connection...\n"
	config = ConfigParser.ConfigParser()
	config.read("conf/application.properties")

	connection = psycopg2.connect(database=config.get("DB", "db.name"), user=config.get("DB", "db.username"), password=config.get("DB", "db.password"), host=config.get("DB", "db.host"), port=config.get("DB", "db.port"))
	
	cursor = connection.cursor()
	cursor.execute("SELECT * FROM bulksmstasks WHERE transactionstatus=5 AND age(current_timestamp, datecreated) < interval '24 years' AND  age(current_timestamp, datecreated) > interval '5 hours'")
	rows = cursor.fetchall()
	
	print "Exporting bulk reports for %s task(s)..." % len(rows)
	for row in rows:
		task_id = row[0]
		target_filename = "%s.csv" % task_id
		print "%s\t%s\t%s\t%s\n" % (row[0], row[1], row[2], row[3])
		# #query = """SELECT t.message_id AS \"MESSAGE ID\" ,t.recipientaddress as \"MSISDN\", CASE t.deliverystatus WHEN 4 THEN 'DeliveredToTerminal' WHEN 3 THEN 'MessageWaiting' WHEN 2 THEN 'DeliveryImpossible' ELSE 'DeliveryUncertain' END AS \"DLR\", creditunits AS \"UNITS\", t.datecreated AS \"CREATED\", t.datemodified AS \"UPDATED\", t.message AS \"MESSAGE\", CASE x.transactionstatus WHEN 0 THEN 'CREATED' WHEN 1 THEN 'PROCESSING' WHEN 2 THEN 'PAUSED' WHEN 3 THEN 'CANCELLED' WHEN 4 THEN 'COMPLETE' WHEN 5 THEN 'COMPLETE' WHEN 6 THEN 'EXPIRED' WHEN 7 THEN 'QUEUED' WHEN 8 THEN 'INSUFFICIENT_CREDIT' WHEN 9 THEN 'SCHEDULED' ELSE 'UKNOWN' END as \"TRANSACTION STATUS\", requestidentifier AS \"MNO Identifier\", p.alphanumeric AS \"SID\", o.orgname AS \"ORGANIZATION\", u.email AS \"USER\" FROM textmessages t INNER JOIN messagetransactions x ON t.messagetransaction_tx_id = x.tx_id JOIN prsservice p ON x.prsservice_id=p.id JOIN organizations o ON x.organization_org_id=o.org_id LEFT JOIN mobitextuser u ON u.user_id=x.mobitextuserid WHERE x.bulksmstask_task_id = %s ORDER by t.datecreated DESC""" % task_id
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
				WHERE x.bulksmstask_task_id = 1103'''

		print "Genrating report for task ID %s: %s" % (task_id, query)
		status = os.system("echo 'COPY (%s) TO STDOUT WITH CSV HEADER' | psql -U %s -h %s %s > %s.csv" % (query, config.get("DB", "db.username"), config.get("DB", "db.host"), config.get("DB", "db.name"), task_id))
		print("Copy command executed with return: %s for task ID %s" % (status, task_id))

		if (status == "0"):
			print "COPY successful. Zipping file %s.csv ..." % task_id
			status = os.system("zip -v %s.zip %s.csv && rm -vs %s.csv" % (task_id, task_id, task_id))
			# Update row to mark export as done
		else:
			print "COPY failed"	
except Exception, e:
	print("Failed in doing this job. %s" % e)
finally:
	if (connection is not None):
		cursor.close()
		connection.close()
		print "Connection closed\n"
		
