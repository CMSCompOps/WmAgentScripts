import MySQLdb
import sys

fname=sys.argv[1]

f=open(fname, 'r')

dbname = "relval3"

conn = MySQLdb.connect(host='localhost', user='relval', passwd="relval")

curs = conn.cursor()

curs.execute("use "+dbname+";")

for line in f:
    wf=line.rstrip('\n')
    curs.execute("delete from workflows where workflow_name = \""+wf+"\";")
