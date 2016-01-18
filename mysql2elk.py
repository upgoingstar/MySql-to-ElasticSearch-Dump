import mysql.connector
import datetime
from elasticsearch import Elasticsearch
import json
import random
import optparse
from datetime import datetime

parser = optparse.OptionParser()
parser.add_option('-u', '--username', action="store", dest="username", help="mysql Username", default="spam")
parser.add_option('-p', '--password', action="store", dest="password", help="mysql password", default="spam")
parser.add_option('-i', '--ip', action="store", dest="host", help="mysql ip", default="spam")
parser.add_option('-d', '--database', action="store", dest="database", help="Database Name", default="spam")
parser.add_option('-e', '--elastdetails', action="store", dest="elastdetails", help="Details of Elasticsearch instance, eg. ip_address:port", default="spam")
options, args = parser.parse_args()


cnx = mysql.connector.connect(user=options.username, password=options.password,
                              host=options.host,
                              database=options.database)

cnxinfoschema = mysql.connector.connect(user=options.username, password=options.password,
                              host=options.host,
                              database='information_schema')

cnx2 = mysql.connector.connect(user=options.username, password=options.password,
                              host=options.host,
                              database=options.database)


data =  {}
finaldata = []
datadict = {}
maincounter = 0
def execute_query(cnx, query):
	cursor = cnx.cursor()
	cursor.execute(query)
	cnx.close()
	return cursor

def dumpToElastic(bodydata, ip, port):
    ES_HOST = {'host': ip, 'port': port}
    es = Elasticsearch(hosts = [ES_HOST])
    es.index(index='frommysql', doc_type="trial", id= random.random(), body=bodydata)

table_names = execute_query(cnx,"show tables") 
maincounter = maincounter + 1
for x in table_names:
	column_list = []
	table_name = json.dumps(x).split('"')[1]
	columnsquery = ("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table_name + "'")
	column_names = execute_query(cnxinfoschema,columnsquery)
	for y in column_names:
		column_list.append(json.dumps(y).split(",")[3])
	data[table_name] = column_list
	tabledata = execute_query(cnx2, "select * from " + table_name)
	for z in tabledata:
		counter = 0
		for abc in z:
			counter = counter + 1
			#print "%s + %s" % (column_list[counter-1], abc)
			datadict['table'] = table_name
			datadict[column_list[counter-1]] = abc
		datadict['timestamp'] = datetime.now()
		if (options.elastdetails != 'spam'):
			print "dumping to ES"
			ip = options.elastdetails.split(":")[0]
			port = options.elastdetails.split(":")[1]
			dumpToElastic(datadict, ip, port)
			print datadict

cnx.close()

