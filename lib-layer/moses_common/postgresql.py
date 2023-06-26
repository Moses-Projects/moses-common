# print("Loaded PostgreSQL module")

import datetime
import json
import re
import logging
import sys

import psycopg2
from psycopg2 import sql as psql

import moses_common.__init__ as common
import moses_common.ui

"""
import moses_common.postgresql
"""

class DBH:
	"""
	dbh = moses_common.postgresql.DBH({
			'host': host,
			'port': port,  # defaults to '5432'
			'dbname': dbname,
			'username': username,
			'password': password,
			'readonly': boolean
		},
		log_level=5,
		dry_run=False
	)
	log_level
		6: All DO statements
		7: All SELECT and DO statements
	"""
	def __init__(self, args, log_level=5, dry_run=False):
		self._dry_run = dry_run
		self.log_level = log_level
		
		missing = []
		self.connect_values = {}
		for item in ['host', 'dbname', 'username', 'password']:
			if args.get(item):
				self.connect_values[item] = args[item]
			else:
				missing.append = item
		
		self.connect_values['port'] = 5432
		if args.get('port'):
			self.connect_values['port'] = args['port']
		
		self._readonly = False
		if 'readonly' in args and args['readonly']:
			print("Enabling readonly")
			self._readonly = True
			if args.get('host_ro'):
				self.connect_values['host'] = args['host_ro']
		
		if len(missing):
			raise AttributeError("Missing connection arg(s) ({})".format(', '.join(missing)))
		
		db = self.connect_values
		try:
			self._conn = psycopg2.connect(
				host = db['host'],
				port = db['port'],
				dbname = db['dbname'],
				user = db['username'],
				password = db['password']
			)
		except psycopg2.DatabaseError as e:
			logging.error(e)
			raise ConnectionError("Unable to connect to database {}@{}:{}/{}".format(db['username'], db['host'], db['port'], db['dbname']))
		
		self._now = datetime.datetime.utcnow()
		self._ui = moses_common.ui.Interface()
	
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
    
	@property
	def now(self):
		return self._now
	
	"""
	dbh.close()
	"""
	def close(self):
		self._conn.close()
		self._conn = None
		return True
	
	
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
			return psql.Literal(value).as_string(self._conn)
		elif type(value) is list:
			qlist = []
			for val in value:
				qlist.append(self._quote_single_value(val))
			return 'ARRAY[' + ','.join(qlist) + ']'
		elif type(value) is type(self._now):
			return psql.Literal(value.isoformat()).as_string(self._conn)
	
	"""
	quoted_value = dbh.quote_like(value)
	"""
	def quote_like(self, value):
		qsearch = self.quote(value)
		qsearch = re.sub(r"^\'", "'%", qsearch)
		qsearch = re.sub(r"\'$", "%'", qsearch)
		return qsearch
	
	"""
	where_conditional = dbh.make_where_conditional(key, value)
	where_conditional = dbh.make_where_conditional(key, value_list)
	where_conditional = dbh.make_where_conditional(key, value_list, 'ilike')
	"""
	def make_where_conditional(self, key, value_list, operator=None, should_quote_identifier=False):
	# 	print("{} = {}".format(key, value_list))
		qkey = key
		if should_quote_identifier:
			qkey = self.identifier(key)
		
		if not operator:
			operator = '='
		
		if type(value_list) is not list:
			value_list = [value_list]
		
		if not len(value_list):
			return None
		
		if operator == 'is' or type(value_list[0]) is bool:
			value = common.convert_to_bool(value_list[0])
			if value is None:
				raise TypeError(f"Value for '{key}' must be a boolean")
			return '{} IS {}'.format(qkey, value)
		elif operator == '@>' or operator == '<@':
			return '{} {} {}'.format(qkey, operator.upper(), self.quote(value_list, quote_arrays=True))
		elif operator == 'like' or operator == 'ilike':
			values = []
			for value in value_list:
				nvalue = common.normalize(value)
				value_words = nvalue.split(' ')
				for word in value_words:
					values.append('{} {} {}'.format(qkey, operator.upper(), self.quote_like(word)))
			return self.join_where_conditionals(values, 'and')
		
		if len(value_list) > 1:
			return '{} IN ({})'.format(qkey, ', '.join(self.quote(value_list)))
		else:
			return '{} {} {}'.format(qkey, operator.upper(), self.quote(value_list[0]))
	
	"""
	joined_where_conditionals = dbh.join_where_conditionals(conditional_list, conjunction)
	"""
	def join_where_conditionals(self, conditional_list, conjunction='and', include_paren=True):
		if conjunction.lower() != 'and' and conjunction.lower() != 'or':
			raise AttributeError("Invalid where conditional conjunction. Can only be 'and' or 'or'.")
		conjunction = ' ' + conjunction.upper() + ' '
		
		if len(conditional_list) > 1:
			if include_paren:
				return '(' + conjunction.join(conditional_list) + ')'
			else:
				return conjunction.join(conditional_list)
		elif len(conditional_list) == 1:
			return conditional_list[0]
		return ''
	
	"""
	where_clause = dbh.make_where_clause({
		"key": "value",
		"key2": ["value2", "value3"],
		"key3": {
			"operator": "ilike",
			"value": ["value4", "value5"]
		}
	}, conjunction)
	"""
	def make_where_clause(self, args, conjunction='and', should_quote_identifier=False):
		where_sql_list = []
		for key, value in args.items():
			if type(value) is dict:
				operator = ''
				should_quote_identifier = False
				if 'should_quote_identifier' in value and value['should_quote_identifier']:
					should_quote_identifier = True
				if 'operator' in value:
					operator = value['operator']
				where_sql_list.append(self.make_where_conditional(key, value['value'], operator, should_quote_identifier))
			else:
				where_sql_list.append(self.make_where_conditional(key, value))
		return self.join_where_conditionals(where_sql_list, conjunction)
		
	"""
	where_clause = dbh.convert_hash_to_where([
		[ "hash_key1" ],
		[ "hash_key2", "field_name2" ],
		[ "hash_key3", "field_name3", "operator" ]
	], hash)
	
	* field_name defaults to hash_key
	* operator defaults to "="
	
	"""
	def convert_hash_to_where(self, field_map, query):
		top_where = {}
		where_list = []
		for field_def in field_map:
			key = field_def[0]
			field = key
			if len(field_def) >= 2:
				field = field_def[1]
			operator = None
			if len(field_def) >= 3:
				operator = field_def[2]
			
			if key in query:
				if type(field) is list:
					conditional_hash = {}
					for fld in field:
						conditional_hash[fld] = { "operator": operator, "value": query[key] }
					where_list.append(self.make_where_clause(conditional_hash, 'or'))
				else:
					top_where[field] = { "operator": operator, "value": query[key] }
		where_sql = self.make_where_clause(top_where)
		if where_sql:
			where_list.append(where_sql)
		if len(where_list):
			return ' WHERE ' + self.join_where_conditionals(where_list, 'and', False)
		return ''
	
	"""
	modifiers_clause = dbh.make_modifiers_clause({
		"sort": sort,
		"order": order,
		"offset": offset,
		"limit": limit
	})
	"""
	def make_modifiers_clause(self, args):
		sql = ''
		if 'sort' in args:
			sql += " ORDER BY " + self.identifier(args['sort'])
		
		if 'order' in args and args['order'] and (args['order'].lower() == 'asc' or args['order'].lower() == 'desc'):
			if sql:
				sql += " " + args['order'].upper()
		
		if 'offset' in args and int(args['offset']) > 0:
			sql += " OFFSET " + str(args['offset'])
		
		if 'limit' in args and int(args['limit']) > 0:
			sql += " LIMIT " + str(args['limit'])
		
		return sql
	
	
	"""
	insert_clause = dbh.make_insert_clause(field_list, data)
	"""
	def make_insert_clause(self, field_list, data):
		if type(data) is not list:
			data = [data]
		
		names = []
		for field in field_list:
			names.append(self.identifier(field))
		
		values = []
		for record in data:
			record_values = []
			for field in field_list:
				value = None
				if field in record:
					value = record[field]
				if re.search(r'\.', field):
					parts = field.split('.')
					if parts[0] in record and type(record[parts[0]]) is dict and parts[1] in record[parts[0]]:
						value = record[parts[0]][parts[1]]
				qvalue = self._quote_single_value(value)
				if not qvalue:
					qvalue = 'NULL'
				record_values.append(qvalue)
			values.append('(' + ', '.join(record_values) + ')')
		sql = '(' + ', '.join(names) + ') VALUES ' + ', '.join(values)
		return sql
	
	"""
	record = dbh.select_base(sql)
	"""
	def select_base(self, sql, args=None):
		if self._log_level >= 7:
			print(sql)
		
		cursor = self._conn.cursor()
		if args:
			cursor.execute(sql, tuple(args))
		else:
			cursor.execute(sql)
		records = cursor.fetchall()
		cursor.close()
		return list(records)
	
	"""
	boolean = dbh.exists(table_name, id_name, id_value)
	"""
	def exists(self, table_name, id_name, id_value):
		sql = "SELECT {} FROM {} WHERE {} = {} LIMIT 1".format(self.identifier(id_name), table_name, self.identifier(id_name), self.quote(id_value))
		results = self.select_base(sql)
		if len(results):
			return True
		return False
	
	"""
	Returns this first field of the first record.
	
	value = dbh.select_value(sql)
	"""
	def select_value(self, sql, args=None):
		results = self.select_base(sql, args)
		if len(results):
			values = list(results[0])
			if len(values):
				return values[0]
		return None
	
	"""
	Returns the first record as a list.
	
	record = dbh.select_as_list(sql)
	"""
	def select_as_list(self, sql, args=None):
		results = self.select_base(sql, args)
		if len(results):
			return list(results[0])
		else:
			return None
	
	"""
	Returns the first record as a hash.
	
	records = dbh.select_as_hash(sql)
	"""
	def select_as_hash(self, sql, args=None):
		if self._log_level >= 7:
			print(sql)
		
		cursor = self._conn.cursor()
		if args:
			cursor.execute(sql, tuple(args))
		else:
			cursor.execute(sql)
		row = cursor.fetchone()
		if not row:
			return
		columns = [desc[0] for desc in cursor.description]
		cursor.close()
		
		record = {}
		for i in range(len(columns)):
			record[columns[i]] = row[i]
		return record
	
	"""
	Returns the first field of all records as a list.
	
	records = dbh.select_column(sql)
	"""
	def select_column(self, sql, args=None):
		results = self.select_base(sql, args)
		records = []
		for row in results:
			records.append(list(row)[0])
		return records
	
	"""
	Returns the first field of all records as the hash value using the specified key as the hash key.
	
	records = dbh.select_column_as_hash(sql, key)
	"""
	def select_column_as_hash(self, sql, key, args=None):
		if self._log_level >= 7:
			print(sql)
		
		cursor = self._conn.cursor()
		if args:
			cursor.execute(sql, tuple(args))
		else:
			cursor.execute(sql)
		rows = cursor.fetchall()
		columns = [desc[0] for desc in cursor.description]
		cursor.close()
		
		records = {}
		for row in rows:
			record = {}
			for i in range(len(columns)):
				record[columns[i]] = row[i]
			records[record[key]] = row[0]
		return records
	
	"""
	Returns each record as a list in a list.
	
	records = dbh.select_as_list_of_lists(sql)
	"""
	def select_as_list_of_lists(self, sql, args=None):
		results = self.select_base(sql, args)
		records = []
		for row in results:
			records.append(list(row))
		return records
	
	"""
	Returns each record as a hash in a list.
	
	records = dbh.select_as_list_of_hashes(sql)
	"""
	def select_as_list_of_hashes(self, sql, args=None):
		if self._log_level >= 7:
			print(sql)
		
		cursor = self._conn.cursor()
		if args:
			cursor.execute(sql, tuple(args))
		else:
			cursor.execute(sql)
		rows = cursor.fetchall()
		columns = [desc[0] for desc in cursor.description]
		cursor.close()
		
		records = []
		for row in rows:
			record = {}
			for i in range(len(columns)):
				record[columns[i]] = row[i]
			records.append(record)
		return records
	
	"""
	Returns each record as a hash in a hash using the specified key as the hash key.
	
	records = dbh.select_as_hash_of_hashes(sql, key)
	"""
	def select_as_hash_of_hashes(self, sql, key, args=None):
		if self._log_level >= 7:
			print(sql)
		
		cursor = self._conn.cursor()
		if args:
			cursor.execute(sql, tuple(args))
		else:
			cursor.execute(sql)
		rows = cursor.fetchall()
		columns = [desc[0] for desc in cursor.description]
		cursor.close()
		
		records = {}
		for row in rows:
			record = {}
			for i in range(len(columns)):
				record[columns[i]] = row[i]
			records[record[key]] = record
		return records
	
	"""
	num_of_inserts = dbh.insert(table_name, data)
	num_of_inserts = dbh.insert(table_name, data, quote_keys=True)
	"""
	def insert(self, table_name, data, quote_keys=False, defaults={}):
		if type(data) is not list:
			data = [data]
		if not len(data):
			return 0
		
		multi = True
		key_list = []
		insert_data = []
		for record in data:
			insert, errors = self.quote_for_table(table_name, record, check_nullable=True, defaults=defaults)
			if len(errors):
				raise ValueError("\n".join(errors))
			insert_data.append(insert)
			if not len(key_list):
				key_list = sorted(insert.keys())
			else:
				record_key_list = sorted(insert.keys())
				if key_list != record_key_list:
					multi = False
		
		# Combined inserts
		if multi:
			# Quote keys
			qkey_list = []
			for key in key_list:
				qkey = key
				if quote_keys:
					qkey = self.identifier(key)
				qkey_list.append(qkey)
			
			# Quote and compile values
			values_list = []
			for record in insert_data:
				value_list = []
				for key in key_list:
					value_list.append(record[key])
				values_list.append("({})".format(', '.join(value_list)))
			sql = "INSERT INTO {} ({}) VALUES {}".format(table_name, ', '.join(key_list), ', '.join(values_list))
			response = self.do(sql)
			return response
		
		# Individual inserts
		cnt = 0
		for record in insert_data:
			qkey_list = []
			value_list = []
			for key in record.keys():
				qkey = key
				if quote_keys:
					qkey = self.identifier(key)
				qkey_list.append(qkey)
				value_list.append(record[key])
			
			sql = "INSERT INTO {} ({}) VALUES ({})".format(table_name, ', '.join(qkey_list), ', '.join(value_list))
			response = self.do(sql)
			cnt += response
		return cnt
	
	"""
	num_of_updates = dbh.update(table_name, key, data)
	num_of_updates = dbh.update(table_name, key, data, quote_keys=True)
	"""
	def update(self, table_name, key_name, data, quote_keys=False):
		if type(data) is not list:
			data = [data]
		
		cnt = 0
		for record in data:
			update, errors = self.quote_for_table(table_name, record)
			if len(errors):
				raise ValueError("\n".join(errors))
			
			set_pairs = []
			where_hash = {}
			for key, value in update.items():
				qkey = key
				if quote_keys:
					qkey = self.identifier(key)
				if key == key_name:
					where_hash[key] = record[key]
				else:
					set_pairs.append("{} = {}".format(qkey, update[key]))
			if not len(where_hash) or not len(set_pairs):
				return
			
			where_clause = self.make_where_clause(where_hash, should_quote_identifier=quote_keys)
			sql = "UPDATE {} SET {} WHERE {}".format(table_name, ', '.join(set_pairs), where_clause)
			
			cnt += self.do(sql)
		return cnt
	
	"""
	num_deleted = dbh.delete(table_name, {
		"key": "value",
		"key2": ["value2", "value3"],
		"key3": {
			"operator": "ilike",
			"value": ["value4", "value5"]
		}
	})
	num_deleted = dbh.delete(table_name, {
		"key": "value"
	}, conjunction='or', quote_keys=True)
	"""
	def delete(self, table_name, where_clause, conjunction='and', should_quote_identifier=False):
		where_clause = self.make_where_clause(where_clause, conjunction=conjunction, should_quote_identifier=should_quote_identifier)
		if not where_clause:
			return 0
		
		sql = "DELETE FROM {} WHERE {}".format(table_name, where_clause)
		
		record = self.do(sql)
		return record
	
	"""
	record = dbh.do(sql)
	"""
	def do(self, sql, args=None):
		if self._dry_run:
			self._ui.dry_run(sql)
			return 1
		if self._log_level >= 6:
			self._ui.body(sql)
		
		sql_parts = re.match(r'(\w+)', sql)
		command = sql_parts.group(1)
		
		if self._readonly:
			raise ConnectionError("Attempting {command} when set to readonly")
		
		cursor = self._conn.cursor()
		if args:
			try:
				cursor.execute(sql, tuple(args))
			except psycopg2.OperationalError as err:
				print("Error: {}".format(err))
				print("Query:", str(cursor.query))
				cursor.close()
				return False
			except psycopg2.ProgrammingError as err:
				print("Error: {}".format(err))
				print("Query:", str(cursor.query))
				cursor.close()
				return False
		else:
			cursor.execute(sql)
		rowcount = cursor.rowcount
		self._conn.commit()
		cursor.close()
		return rowcount
	
	
	
	
	"""
	insert_data, errors = dbh.quote_for_table(table_name, data)
	insert_data, errors = dbh.quote_for_table(table_name, data, check_nullable=True, defaults={})
	"""
	def quote_for_table(self, table_name, data, check_nullable=False, defaults={}):
		"""
		bigint
		bigserial
		bit [ (n) ]
		bit varying [ (n) ]
		boolean
		box
		bytea
		character [ (n) ]
		character varying [ (n) ]
		cidr
		circle
		date
		double precision
		inet
		integer
		interval [ fields ] [ (p) ]
		json
		jsonb
		line
		lseg
		macaddr
		macaddr8
		money
		numeric [ (p, s) ]
		path
		pg_lsn
		pg_snapshot
		point
		polygon
		real
		smallint
		smallserial
		serial
		text
		time [ (p) ] [ without time zone ]
		time [ (p) ] with time zone
		timestamp [ (p) ] [ without time zone ]
		timestamp [ (p) ] with time zone
		tsquery
		tsvector
		txid_snapshot
		uuid
		xml
		"""
		columns = self.get_column_info(table_name)
		insert = {}
		errors = []
		
		# Fill data with defaults
		if len(defaults):
			for key, value in defaults.items():
				if key in data:
					if type(value) is bool:
						if value:
							data[key] = True
						continue
					elif common.is_datetime_type(data[key]):
						continue
					elif data[key]:
						continue
				data[key] = value
		
		# Convert each value per column
		for column_name, column in columns.items():
			required = False
			if column['is_nullable'] == 'NO':
				required = True
			data_type = column['data_type'].lower()
			
			if column_name not in data:
				if check_nullable and required:
					errors.append(f"'{column_name}' is required")
				continue
			
			value = data[column_name]
			if data_type == 'boolean':
				value = common.convert_to_bool(value)
				if value is None:
					errors.append(f"'{column_name}' cannot convert to a boolean")
					continue
				insert[column_name] = self._quote_single_value(value)
			elif re.match(r'date', data_type):
				if value.lower() in ['current_date', 'now()']:
					value = self.get_current_date()
					insert[column_name] = self._quote_single_value(value)
				else:
					date_obj = common.convert_string_to_date(value)
					if date_obj is not None:
						insert[column_name] = self._quote_single_value(date_obj.isoformat())
			elif data_type in ['decial', 'numeric', 'double precision']:
				value = common.convert_to_float(value)
				if value is None:
					errors.append(f"'{column_name}' cannot convert to an float")
					continue
				insert[column_name] = self._quote_single_value(value)
			elif data_type in ['integer', 'smallint', 'bigint']:
				value = common.convert_to_int(value)
				if value is None:
					errors.append(f"'{column_name}' cannot convert to an integer")
					continue
				if data_type == 'smallint' and (value < -32768 or value > 32767):
					errors.append(f"'{column_name}' out of range")
					continue
				insert[column_name] = self._quote_single_value(value)
			elif data_type in ['text', 'character', 'character varying']:
				value = str(value)
				if re.match(r'uuid_generate', value):
					value = self.generate_uuid()
				if column['character_maximum_length'] and len(value) > column['character_maximum_length']:
					errors.append(f"'{column_name}' out of range")
					continue
				insert[column_name] = self._quote_single_value(value)
			elif re.match(r'timestamp', data_type):
				if common.is_datetime(value):
					insert[column_name] = self._quote_single_value(value.isoformat())
				elif common.is_date(value):
					datetime_obj = common.convert_string_to_datetime(value)
					insert[column_name] = self._quote_single_value(datetime_obj.isoformat())
				elif type(value) is str and value.lower() in ['current_timestamp', 'current_time', 'now()']:
					value = self.get_current_timestamp()
					insert[column_name] = self._quote_single_value(value)
				else:
					datetime_obj = common.convert_string_to_datetime(value)
					if datetime_obj is not None:
						insert[column_name] = self._quote_single_value(datetime_obj.isoformat())
			elif re.match(r'time', data_type):
				if value.lower() in ['current_timestamp', 'current_time', 'now()']:
					value = self.get_current_time()
					insert[column_name] = self._quote_single_value(value)
				else:
					time_obj = common.convert_string_to_time(value)
					if time_obj is not None:
						insert[column_name] = self._quote_single_value(time_obj.isoformat())
			elif data_type == 'uuid':
				if not common.is_uuid(value):
					errors.append(f"'{column_name}' is not a valid uuid")
					continue
				insert[column_name] = self._quote_single_value(value)
			elif data_type == 'array':
				if type(value) is not list:
					errors.append(f"'{column_name}' is not an array")
					continue
				new_array = []
				if column['udt_name'] == '_text':
					for element in value:
						new_array.append(self._quote_single_value(str(element)))
					value = 'ARRAY[' + ','.join(new_array) + ']::text[]'
				else:
					errors.append(f"'{column_name}' array type of {column['udt_name']} is not supported")
				insert[column_name] = value
			else:
				errors.append(f"'{column_name}' data type of {data_type} is not supported")
		
		return insert, errors
	
	
	
	"""
	column_hash = dbh.get_column_info(table_name)
	"""
	def get_column_info(self, table_name):
		schema = 'public'
		table = table_name
		if re.search(r'\.', table_name):
			parts = table_name.split('.')
			schema = parts[0]
			table = parts[1]
		sql = f"SELECT * FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table}'"
		return self.select_as_hash_of_hashes(sql, 'column_name')
	
	"""
	uuid = dbh.generate_uuid()
	"""
	def generate_uuid(self):
		return self.select_value("SELECT uuid_generate_v4()")
	
	"""
	timestamp = dbh.get_current_timestamp()
	"""
	def get_current_timestamp(self):
		return self.select_value("SELECT CURRENT_TIMESTAMP")
	
	"""
	time = dbh.get_current_time()
	"""
	def get_current_time(self):
		return self.select_value("SELECT CURRENT_TIME")
	
	"""
	date = dbh.get_current_date()
	"""
	def get_current_date(self):
		return self.select_value("SELECT CURRENT_DATE")
	
	
	
	"""
	record = dbh.create_table(table_name, create_sql)
	"""
	def create_table(self, table_name, sql):
		return self.do(sql)
	
	"""
	record = dbh.drop_table(table_name)
	"""
	def drop_table(self, table_name):
		sql = "DROP TABLE {}".format(table_name)
		return self.do(sql)
	
	
	
	"""
	success = dbh.is_valid_role_name(role_name)
	"""
	def is_valid_role_name(self, role_name):
		if type(role_name) is not str:
			return False
		if len(role_name) < 1 or len(role_name) > 63:
			return False
		if re.match(r'[a-z_][a-zA-Z0-9_]*$', role_name):
			return True
		return False
	
	"""
	record = dbh.create_role(role_name, password)
	"""
	def create_role(self, role_name, password):
		if not self.is_valid_role_name(role_name):
			return False
		cursor = self._conn.cursor()
		cursor.execute("SELECT oid FROM pg_roles WHERE rolname = %s", (role_name,))
		role_oid = cursor.fetchone()
		if role_oid:
			return True
		
		if self._readonly:
			raise ConnectionError("Attempting CREATE when set to readonly")
		
		sql = "CREATE ROLE {} WITH LOGIN PASSWORD %s".format(role_name)
		try:
			cursor.execute(sql, (password,))
		except psycopg2.OperationalError as err:
			print("Error {}: {}".format(type(err), err))
			cursor.close()
			return False
		
		self._conn.commit()		
		cursor.close()
		return True
	
	"""
	record = dbh.drop_role(role_name)
	"""
	def drop_role(self, role_name):
		if not self.is_valid_role_name(role_name):
			return False
		cursor = self._conn.cursor()
		cursor.execute("SELECT oid FROM pg_roles WHERE rolname = %s", (role_name,))
		role_oid = cursor.fetchone()
		if not role_oid:
			return True
		
		if self._readonly:
			raise ConnectionError("Attempting DROP when set to readonly")
		
		sql = "DROP ROLE {}".format(role_name)
		try:
			cursor.execute(sql)
		except psycopg2.OperationalError as err:
			print("Error {}: {}".format(type(err), err))
			cursor.close()
			return False
		
		self._conn.commit()	
		cursor.close()
		return True
	
