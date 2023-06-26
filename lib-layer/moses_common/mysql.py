# print("Loaded MySQL module")

import json
import re
import pymysql.cursors
import moses_common.__init__ as common



"""
import moses_common.mysql
"""

class DBH:
	"""
	dbh = moses_common.mysql.DBH({
		'host': host,
		'port': port,  # defaults to '3306'
		'db_name': db_name,
		'username': username,
		'password': password
	})
	"""
	def __init__(self, args):
		missing = []
		self.connect_values = {}
		for item in ['host', 'username', 'password']:
			if item in args:
				self.connect_values[item] = args[item]
			else:
				missing.append = item
		if 'db_name' in args:
			self.connect_values['db_name'] = args['db_name']
		else:
			self.connect_values['db_name'] = None
		if 'port' in args:
			self.connect_values['port'] = args['port']
		else:
			self.connect_values['port'] = 3306
		
		if len(missing):
			raise AttributeError("Missing connection arg(s) ({})".format(', '.join(missing)))
		
		db = self.connect_values
		self.conn = pymysql.connect(
			host = db['host'],
			database = db['db_name'],
			user = db['username'],
			password = db['password'],
			cursorclass = pymysql.cursors.DictCursor
		)
		if not self.conn:
			raise AttributeError("Unable to connect to database {}@{}:{}/{}".format(db['username'], db['host'], db['port'], db['db_name']))
	
	"""
	dbh.commit()
	"""
	def commit(self):
		self.conn.commit()
	
	"""
	record = dbh.select_one(sql)
	"""
	def select_one(self, sql):
		cursor = self.conn.cursor()
		cursor.execute(sql)
		return cursor.fetchone()
	
	"""
	records = dbh.select_all(sql)
	"""
	def select_all(self, sql):
		cursor = self.conn.cursor()
		cursor.execute(sql)
		return cursor.fetchall()
	
	
	"""
	record = dbh.do(sql)
	"""
	def do(self, sql):
		cursor = self.conn.cursor()
# 		print("sql {}: {}".format(type(sql), sql))
		cursor.execute(sql)
		self.commit()
	
	"""
	ADMIN FUNCTIONS
	Only works if no db_name is given at init.
	"""
	
	def is_database(self, database_name):
		if self.connect_values['db_name']:
			return False
		cursor = self.conn.cursor()
		cursor.execute('SHOW DATABASES')
		databases = cursor.fetchall()
		for db in databases:
			if 'Database' in db and db['Database'] == database_name:
				return True
		return False
		
	def is_user(self, username):
		if self.connect_values['db_name']:
			return False
		cursor = self.conn.cursor()
		cursor.execute('SELECT User FROM mysql.user')
		users = cursor.fetchall()
		for user in users:
			if 'User' in user and user['User'] == username:
				return True
		return False
		
	def has_grant(self, username):
		if self.connect_values['db_name']:
			return False
		cursor = self.conn.cursor()
		qusername = self.conn.escape(username)
		cursor.execute("SHOW GRANTS FOR {}@'%'".format(qusername))
		grants = cursor.fetchall()
		if len(grants) == 2:
			return True
		return False
		
	"""
	success = create_database(database_name)
	"""
	def create_database(self, database_name):
		if self.connect_values['db_name']:
			return False
		cursor = self.conn.cursor()
		sql = 'CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci'.format(database_name)
		cursor.execute(sql)
		if self.is_database(database_name):
			return True
		return False
	
	"""
	success = drop_database(database_name)
	"""
	def drop_database(self, database_name):
		if self.connect_values['db_name']:
			return False
		cursor = self.conn.cursor()
		sql = 'DROP DATABASE IF EXISTS {}'.format(database_name)
		cursor.execute(sql)
		if self.is_database(database_name):
			return False
		return True
	
	"""
	success = create_user(username, password)
	"""
	def create_user(self, username, password):
		if self.connect_values['db_name']:
			return False
		cursor = self.conn.cursor()
		qusername = self.conn.escape(username)
		qpassword = self.conn.escape(password)
		sql = "CREATE USER {}@'%' IDENTIFIED BY {}".format(qusername, qpassword)
		cursor.execute(sql)
		if self.is_user(username):
			return True
		return False
	
	"""
	success = grant_user(username, database_name)
	"""
	def grant_user(self, username, database_name):
		if self.connect_values['db_name']:
			return False
		cursor = self.conn.cursor()
		qusername = self.conn.escape(username)
		sql = "GRANT ALL PRIVILEGES ON `{}`.* TO {}@'%'".format(database_name, qusername)
		cursor.execute(sql)
		if self.has_grant(username):
			return True
		return False
	
	
