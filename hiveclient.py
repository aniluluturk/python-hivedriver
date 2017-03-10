# hiveclient.py

import pyhs2

class Result:
	schema = None
	data = None
	
	def print_table(self, table):
		col_width = [max(len(str(x)) for x in col) for col in zip(*table)]
		ll = None
		for idx,line in enumerate(table):
			l = " | ".join("{:{}}".format(x, col_width[i]) for i, x in enumerate(line))
			ll =l
			print "| " + l + " |"
			if idx==0:
				print "-"*(len(ll)+4)
		return len(ll) + 4 # length of a single line
	
	def __init__(self, schema, data):
		self.schema = schema
		self.data = data
	
	def pprint(self):
		colNames = [h["columnName"] for h in self.schema]
		colNames = [colNames]
		self.print_table(colNames + self.data)


class Client:

	conn = None
	hostname = None
	port = None
	cursor = None

	def __init__(self, hostname, port = 19123):
		self.hostname = hostname
		self.port = port
   
	def connect(self, getCursor=True):
		print "Connecting to  %s on port %d" % (self.hostname, self.port)
		self.conn = pyhs2.connect(host=self.hostname, authMechanism='PLAIN',user='root', port=self.port)
		if getCursor:
			self.cursor = self.conn.cursor()

	def printInfo(self):
		print "Hostname : ", self.hostname,  ", Port: ", str(self.port)
		
	def isConnected(self):
		return (not self.conn == None)
		
	def getCursor(self):
		return self.cursor
		
	def disconnect(self):
		self.conn.close()
		
	def sql(self, sqlStr):
		res = self.cursor.execute(sqlStr)
		resObj = Result(self.cursor.getSchema(), self.cursor.fetch())
		return resObj
		
	def selectAll(self, tableName, cols = None, select=""):
		cols = "*"
		if not cols == None:
			cols = ",".join(cols)
		query = "SELECT %s FROM %s" % (cols, tableName)
		if select != "":
			query = query + " WHERE " + select  
		return self.sql(query)
		
	
	
