# print("Loaded MySQL module")

import datetime
import json
import re
import pymysql.cursors
import moses_common.__init__ as common
import moses_common.ui



"""
import moses_common.mysql
"""

class DBH:
	"""
	dbh = moses_common.mysql.DBH({
			'host': host,
			'port': port,  # defaults to '3306'
			'dbname': db_name,
			'username': username,
			'password': password
		},
		default_schema: 'dbname',
		ui=None,
		log_level=5,
		dry_run=False
	)
	log_level
		6: All DO statements
		7: All SELECT and DO statements
	"""
	def __init__(self, args, default_schema='dbname', ui=None, log_level=5, dry_run=False):
		self.dry_run = dry_run
		self.log_level = log_level
		self.ui = ui or moses_common.ui.Interface()
		self.now = datetime.datetime.utcnow()
		self.default_schema = default_schema
		self._schema = None
		
		missing = []
		self.connect_values = {}
		for item in ['host', 'username', 'password']:
			if item in args:
				self.connect_values[item] = args[item]
			else:
				missing.append(item)
		if 'dbname' in args:
			self.connect_values['dbname'] = args['dbname']
		elif 'db_name' in args:
			self.connect_values['dbname'] = args['db_name']
		else:
			self.connect_values['dbname'] = None
		if 'port' in args:
			self.connect_values['port'] = common.convert_to_int(args['port'])
		else:
			self.connect_values['port'] = 3306
		
		if len(missing):
			raise AttributeError("Missing {}: {}".format(common.plural(len(missing), 'connection arg'), common.conjunction(missing, conj='and', quote="'")))
		
		db = self.connect_values
		try:
			self._conn = pymysql.connect(
				host = db['host'],
				port = db['port'],
				database = db['dbname'],
				user = db['username'],
				password = db['password'],
# 				cursorclass = pymysql.cursors.DictCursor
			)
		except pymysql.err.OperationalError as e:
			raise ConnectionError("Unable to connect to mysql database {}@{}:{}:{}".format(db['username'], db['host'], db['port'], db['dbname']))
		
		self.table_data = {}
		
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
    
	@property
	def schema(self):
		if not self._schema:
			self._schema = 'public'
			if self.default_schema == 'dbname':
				self._schema = self.connect_values['dbname']
		return self._schema
	
	"""
	dbh.commit()
	"""
	def commit(self):
		self._conn.commit()
	
	
	# SQL Generation
	
	"""
	quoted_identifier = dbh.identifier(value)
	"""
	def identifier(self, value):
		return '"{}"'.format(value)
	
	"""
	quoted_value = dbh.quote('one')
		"'one'"
	quoted_value_list = dbh.quote(['one', 'two'])
		["'one'", "'two'"]
	quoted_value_sql = dbh.quote(['one', 'two'], quote_arrays=True)
		"ARRAY['one', 'two']"
	"""
	def quote(self, value_list, quote_arrays=False):
		if type(value_list) is list:
			if quote_arrays:
				return self._quote_single_value(value_list)
			
			qvalues = []
			for value in value_list:
				qvalues.append(self._quote_single_value(value))
			return qvalues
		return self._quote_single_value(value_list)
	
	def _quote_single_value(self, value):
		if value is None:
			return 'NULL'
		elif type(value) is bool:
			if value:
				return 'TRUE'
			else:
				return 'FALSE'
		elif type(value) is float or type(value) is int:
			return str(value)
		elif type(value) is str:
			if value.lower() in ['current_time', 'current_timestamp', 'now()']:
				return value
			elif re.match(r'uuid_generate_\w+\(\)', value):
				return value
			elif re.match(r'ARRAY\[\]', value):
				return value
			return f"'{self._conn.escape_string(value)}'"
		elif type(value) is list:
			qlist = []
			for val in value:
				qlist.append(self._quote_single_value(val))
			return 'ARRAY[' + ','.join(qlist) + ']'
		elif type(value) is type(self.now):
			return f"'{self._conn.escape_string(value.isoformat())}'"
	
	"""
	quoted_value = dbh.quote_like(value)
	"""
	def quote_like(self, value):
		qsearch = self.quote(value)
		qsearch = re.sub(r"^\'", "'%", qsearch)
		qsearch = re.sub(r"\'$", "%'", qsearch)
		return qsearch
	
	# Get table info
	
	"""
	column_hash = dbh.get_column_info(table_name, schema=None)
	"""
	def get_column_info(self, table_name, schema=None, record_type='dict'):
		schema, table_name = self.split_schema_from_table_name(table_name, schema=schema)
		if self.table_data.get(schema) and self.table_data[schema].get(table_name):
			return self.table_data[schema][table_name]
		sql = f"SELECT * FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table_name}'"
		if schema not in self.table_data:
			self.table_data[schema] = {}
		column_map = self.select_as_hash_of_hashes(sql, 'COLUMN_NAME')
		new_column_map = {}
		new_column_list = []
		for field, definition in column_map.items():
			new_column_map[field] = {}
			for key, value in definition.items():
				new_column_map[field][key.lower()] = value
			new_column_list.append(new_column_map[field])
		self.table_data[schema][table_name] = new_column_map
		if record_type == 'list':
			return new_column_list
		return self.table_data[schema][table_name]
	
	
	# Get records
	
# 	"""
# 	record = dbh.select_one(sql)
# 	"""
# 	def select_one(self, sql):
# 		cursor = self._conn.cursor()
# 		cursor.execute(sql)
# 		return cursor.fetchone()
# 	
# 	"""
# 	records = dbh.select_all(sql)
# 	"""
# 	def select_all(self, sql):
# 		cursor = self._conn.cursor()
# 		cursor.execute(sql)
# 		return cursor.fetchall()
	
	# Get records
	
	"""
	record = dbh.select_base(sql)
	"""
	def select_base(self, sql, args=None, record_type='list'):
		if self._log_level >= 7:
			print(sql)
		
		cursor = self._conn.cursor()
		if args:
			cursor.execute(sql, tuple(args))
		else:
			cursor.execute(sql)
		rows = cursor.fetchall()
		columns = None
		if record_type == 'hash':
			columns = [desc[0] for desc in cursor.description]
		cursor.close()
		
		# Return list of lists
		if record_type == 'list':
			return list(rows)
		
		# Return list of hashes
		records = []
		for row in rows:
			record = {}
			for i in range(len(columns)):
				record[columns[i]] = row[i]
			records.append(record)
		return records
	
	"""
	Returns this first field of the first record.
	
	value = dbh.select_value(sql)
	"record1-field1"
	"""
	def select_value(self, sql, args=None):
		results = self.select_base(sql, args)
		if len(results):
			values = list(results[0])
			if len(values):
				return values[0]
		return None
	
	"""
	Returns the first field of all records as a list.
	
	records = dbh.select_column(sql)
	[ "record1-field1", "record2-field1", ... ]
	"""
	def select_column(self, sql, args=None):
		results = self.select_base(sql, args)
		records = []
		for row in results:
			records.append(row[0])
		return records
	
	"""
	Returns each record as a hash in a list.
	
	records = dbh.select_as_list_of_hashes(sql)
	[ { record1 }, { record2 }, ... ]
	"""
	def select_as_list_of_hashes(self, sql, args=None):
		return self.select_base(sql, args, record_type='hash')
	
	"""
	Returns each record as a hash in a hash using the specified key as the hash key.
	hash key must be unique or records will be missed.
	
	records = dbh.select_as_hash_of_hashes(sql, key)
	{
		"hash_key1": { record1 },
		"hash_key2": { record2 },
		...
	}
	"""
	def select_as_hash_of_hashes(self, sql, key, args=None):
		rows = self.select_base(sql, args, record_type='hash')
		
		records = {}
		for row in rows:
			records[row[key]] = row
		return records
	
	
	"""
	record = dbh.do(sql)
	"""
	def do(self, sql):
		if self.dry_run:
			self.ui.dry_run(sql)
			return 1
		if self.log_level >= 6:
			self.ui.body(sql)
		
		cursor = self._conn.cursor()
# 		print("sql {}: {}".format(type(sql), sql))
		cursor.execute(sql)
		self.commit()
	
	"""
	ADMIN FUNCTIONS
	Only works if no db_name is given at init.
	"""
	
	"""
	schema, table_name = dbh.split_schema_from_table_name(full_table_name, schema=None)
	"""
	def split_schema_from_table_name(self, full_table_name, schema=None):
		if not schema:
			schema = self.schema
		table_name = full_table_name
		if re.search(r'\.', full_table_name):
			parts = full_table_name.split('.')
			if not parts or len(parts) > 2:
				raise ValueError(f"Table name '{full_table_name}' can not be split into schema/table")
			schema = parts[0]
			table_name = parts[1]
		return schema, table_name
	
	
	def is_database(self, database_name):
		if self.connect_values['db_name']:
			return False
		cursor = self._conn.cursor()
		cursor.execute('SHOW DATABASES')
		databases = cursor.fetchall()
		for db in databases:
			if 'Database' in db and db['Database'] == database_name:
				return True
		return False
		
	def is_user(self, username):
		if self.connect_values['db_name']:
			return False
		cursor = self._conn.cursor()
		cursor.execute('SELECT User FROM mysql.user')
		users = cursor.fetchall()
		for user in users:
			if 'User' in user and user['User'] == username:
				return True
		return False
		
	def has_grant(self, username):
		if self.connect_values['db_name']:
			return False
		cursor = self._conn.cursor()
		qusername = self._conn.escape(username)
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
		if self.dry_run:
			self.ui.dry_run(f"CREATE DATABASE {database_name}")
			return 1
		
		cursor = self._conn.cursor()
		sql = 'CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci'.format(database_name)
		if self.log_level >= 6:
			self.ui.body(sql)
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
		if self.dry_run:
			self.ui.dry_run(f"DROP DATABASE {database_name}")
			return 1
		
		cursor = self._conn.cursor()
		sql = 'DROP DATABASE IF EXISTS {}'.format(database_name)
		if self.log_level >= 6:
			self.ui.body(sql)
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
		if self.dry_run:
			self.ui.dry_run(f"CREATE USER {username}")
			return 1
		
		cursor = self._conn.cursor()
		qusername = self._conn.escape(username)
		qpassword = self._conn.escape(password)
		sql = "CREATE USER {}@'%' IDENTIFIED BY {}".format(qusername, qpassword)
		if self.log_level >= 6:
			self.ui.body(sql)
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
		if self.dry_run:
			self.ui.dry_run(f"GRANT ALL PRIVILEGES ON {database_name} TO {username}")
			return 1
		
		cursor = self._conn.cursor()
		qusername = self._conn.escape(username)
		sql = "GRANT ALL PRIVILEGES ON `{}`.* TO {}@'%'".format(database_name, qusername)
		if self.log_level >= 6:
			self.ui.body(sql)
		cursor.execute(sql)
		if self.has_grant(username):
			return True
		return False
	
	"""
	schema_list = dbh.get_schema_list()
	"""
	def get_schema_list(self):
		return self.select_column(f"SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")
	
	"""
	schema = dbh.get_table_schema(table_name)
	"""
	def get_table_schema(self, table_name):
		qtable = self.quote(table_name)
		schemas = self.select_column(f"SELECT table_schema FROM information_schema.tables WHERE table_name = {qtable} ORDER BY table_name")
		if len(schemas) == 1:
			return schemas[0]
		return None
	
	"""
	table_list = dbh.get_table_list(schema)
	"""
	def get_table_list(self, schema=None):
		if not schema:
			schema = self.schema
		qschema = self.quote(schema)
		return self.select_column(f"SELECT table_name FROM information_schema.tables WHERE table_schema = {qschema} ORDER BY table_name")
	
	"""
	boolean = dbh.is_table(table_name, schema=None)
	"""
	def is_table(self, table_name, schema=None):
		schema, table_name = self.split_schema_from_table_name(table_name, schema=schema)
		answer = self.select_value("SELECT table_name FROM information_schema.tables WHERE table_schema = {} AND table_name = {}".format(self.quote(schema), self.quote(table_name)))
		if answer:
			return True
		return False
	
	
