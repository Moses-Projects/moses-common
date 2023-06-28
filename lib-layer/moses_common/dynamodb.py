# print("Loaded DynamoDB module")

import boto3
import datetime
import re
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

import moses_common.__init__
import moses_common.ui

boto3_client = boto3.client('dynamodb', region_name="us-west-2")

def is_valid_name(name):
	if type(name) is not str:
		return False
	if not re.match(r'[a-zA-Z0-9_.-]{3,255}$', name):
		return False
	return True

class Table:
	"""
	import moses_common.dynamodb
	table = moses_common.dynamodb.Table(table_name)
	"""
	def __init__(self, table_name, log_level=5, dry_run=False):
		self._dry_run = dry_run
		self.log_level = log_level
		
		if not is_valid_name(table_name):
			raise AttributeError("Invalid table name")
		self._name = table_name
		self._indexes = None
		self._attributes = None
		self._partition_key = None
		self._sort_key = None
		if self.load():
			self._exists = True
		else:
			self._exists = False
		self._ui = moses_common.ui.Interface()
		
	'''
	{
		"AttributeDefinitions": [
			{
				"AttributeName": "token",
				"AttributeType": "S"
			}
		],
		"CreationDateTime": "2021-07-09 20:15:14",
		"ItemCount": "65",
		"KeySchema": [
			{
				"AttributeName": "token",
				"KeyType": "HASH"
			}
		],
		"ProvisionedThroughput": {},
		"TableArn": "arn:aws:dynamodb:us-west-2:884259710201:table/reports-token",
		"TableId": "2c978ee6-beb9-4234-97de-a875796cd95b",
		"TableName": "reports-token",
		"TableSizeBytes": "5708",
		"TableStatus": "ACTIVE"
	}
	'''
	def load(self):
		response = boto3_client.describe_table(
			TableName = self.name
		)
# 		print("response {}: {}".format(type(response), response))
		if moses_common.is_success(response) and 'Table' in response and type(response['Table']) is dict:
			self._info = response['Table']
			if 'AttributeDefinitions' in response['Table'] and type(response['Table']['AttributeDefinitions']) is list:
				self._attributes = {}
				for attribute_info in response['Table']['AttributeDefinitions']:
					if 'AttributeName' in attribute_info:
						self._attributes[attribute_info['AttributeName']] = Attribute(self, attribute_info, log_level=self.log_level, dry_run=self._dry_run)
				if 'KeySchema' in response['Table'] and type(response['Table']['KeySchema']) is list:
					for i in range(len(response['Table']['KeySchema'])):
						key_info = response['Table']['KeySchema'][i]
						if key_info and type(key_info) is dict:
							if 'AttributeName' in key_info:
								self._attributes[key_info['AttributeName']]._info['key_type'] = key_info['KeyType']
						if i == 0:
							self._partition_key = self._attributes[key_info['AttributeName']]
						if i == 1:
							self._sort_key = self._attributes[key_info['AttributeName']]
			if 'GlobalSecondaryIndexes' in response['Table'] and type(response['Table']['GlobalSecondaryIndexes']) is list:
				self._indexes = {}
				for index_info in response['Table']['GlobalSecondaryIndexes']:
					index = Index(self, index_info, log_level=self.log_level, dry_run=self._dry_run)
					self._indexes[index.name] = index
			return True
		return False
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = moses_common.normalize_log_level(value)
	
	@property
	def exists(self):
		return self._exists
	
	@property
	def name(self):
		return self._name
	
	@property
	def partition_key(self):
		return self._partition_key

	@property
	def sort_key(self):
		return self._sort_key
	
	@property
	def arn(self):
		if not self._exists and 'TableArn' not in self._info:
			return None
		return self._info['TableArn']
	
	@property
	def item_count(self):
		return self._info['ItemCount']
	
	@property
	def indexes(self):
		if not self._exists or type(self._indexes) is not dict:
			return None
		return self._indexes
	
	def get_ts(self):
		return datetime.datetime.utcnow().isoformat(' ')
	
	def get_iso_timestamp(self):
		return datetime.datetime.now().isoformat()
	
	def convert_to_num(self, value, use_none=False):
		if type(value) is int or type(value) is float:
			return value
		elif type(value) is str:
			value = value.strip()
			if re.match(r'[0-9]+$', value):
				return int(value)
			elif re.match(r'[.-0-9]+$', value):
				return float(value)
			elif use_none:
				return None
			else:
				return int(0)
		elif type(value) is bool and value:
			return int(1)
		elif use_none:
			return None
		else:
			return int(0)
		
	def convert_to_int(self, value, use_none=False):
		return int(self.convert_to_num(value, use_none=False))
	
	def convert_to_pos_int(self, value, use_none=False):
		number = int(self.convert_to_num(value, use_none=False))
		if number and number >= 0:
			return number
		if use_none:
			return None
		else:
			return int(0)
	
	def convert_to_attribute_value(self, value, attribute_type=None):
		if attribute_type:
			if attribute_type == 'S':
				return { 'S': str(value) }
			elif attribute_type == 'N':
				return { 'N': str(self.convert_to_num(value)) }
			elif attribute_type == 'M':
				if type(value) is dict:
					new_map = {}
					for key, item in value.items():
						new_map[key] = self.convert_to_attribute_value(item)
					return { 'M': new_map }
			elif attribute_type == 'L':
				if type(value) is list:
					new_list = []
					for item in value:
						new_list.append(self.convert_to_attribute_value(item))
					return { 'L': new_list }
			elif attribute_type == 'NULL':
				return { 'NULL': True }
			elif attribute_type == 'BOOL':
				if value:
					return { 'BOOL': True }
				else:
					return { 'BOOL': False }
		if type(value) is str:
			return self.convert_to_attribute_value(value, 'S')
		elif type(value) is int or type(value) is float:
			return self.convert_to_attribute_value(value, 'N')
		elif type(value) is dict:
			return self.convert_to_attribute_value(value, 'M')
		elif type(value) is list:
			return self.convert_to_attribute_value(value, 'L')
		elif value is None:
			return self.convert_to_attribute_value(value, 'NULL')
		elif type(value) is bool:
			return self.convert_to_attribute_value(value, 'BOOL')
		return self.convert_to_attribute_value(value, 'S')
	
	def convert_to_item(self, record):
		if type(record) is dict:
			new_record = {}
			for key, value in record.items():
				new_record[key] = self.convert_to_attribute_value(value)
			return new_record
		if type(record) is list:
			new_record = []
			for value in record:
				new_record.append(self.convert_to_item(value))
			return new_record
	
	def convert_from_attribute_value(self, attribute_value):
		if type(attribute_value) is not dict:
			return attribute_value
		if 'NULL' in attribute_value:
			return None
		elif 'BOOL' in attribute_value:
			return attribute_value['BOOL']
		elif 'S' in attribute_value:
			return str(attribute_value['S'])
		elif 'N' in attribute_value:
			return int(attribute_value['N'])
		elif 'M' in attribute_value:
			new_map = {}
			for key, item in attribute_value['M'].items():
				new_map[key] = self.convert_from_attribute_value(item)
			return new_map
		elif 'L' in attribute_value:
			new_list = []
			for item in attribute_value['L']:
				new_list.append(self.convert_from_attribute_value(item))
			return new_list
		return None
	
	def convert_from_item(self, record):
		if type(record) is dict:
			new_record = {}
			for key, value in record.items():
				new_record[key] = self.convert_from_attribute_value(value)
			return new_record
		if type(record) is list:
			new_record = []
			for value in record:
				new_record.append(self.convert_from_item(value))
			return new_record
	
	
	"""
	record = table.get_item(key_value, sort_value=None)
	"""
	def get_item(self, key_value, sort_value=None):
		key_object = {
			self._partition_key.name: self.convert_to_attribute_value(key_value, self._partition_key.type)
		}
		if self._sort_key:
			if sort_value:
				key_object[self._sort_key.name] = self.convert_to_attribute_value(sort_value, self._sort_key.type)
			else:
				raise AttributeError("Table '{}' with sort key '{}' requires a sort key value".format(self._name, self._sort_key.name))
		try:
			response = boto3_client.get_item(
				TableName = self._name,
				Key = key_object
			)
		except ClientError as e:
			if self.log_level >= 7:
				print("error:", e)
# 			raise ConnectionError("Failed to get item from DynamodDB " + self.name)
			pass
		else:
			if self.log_level >= 7:
				print("response:", response)
			if moses_common.is_success(response) and 'Item' in response:
				return self.convert_from_item(response['Item'])
	
	"""
	max_id = table.get_max_range_value(key_value)
	"""
	def get_max_range_value(self, key_value):
		if not self._sort_key:
			raise AttributeError("Table '{}' does not have a sort key".format(self._name))
		if self._sort_key.key_type != 'RANGE':
			raise AttributeError("Table '{}' sort key '{}' is not of type RANGE".format(self._name, self._sort_key.name))
		key_condition = '#partition_key = :key_value'
		attribute_names = {"#partition_key":self._partition_key.name}
		attribute_values = {":key_value":self.convert_to_attribute_value(key_value)}
		projection = '{},{}'.format(self._partition_key.name, self._sort_key.name)
		try:
			response = boto3_client.query(
				TableName = self.name,
				ProjectionExpression = projection,
				KeyConditionExpression = key_condition,
				ExpressionAttributeNames = attribute_names,
				ExpressionAttributeValues = attribute_values,
				ScanIndexForward = False,
				Limit = 1
			)
		except ClientError as e:
			if self.log_level >= 7:
				print("error:", e)
# 			raise ConnectionError("Failed to get item from DynamodDB " + self.name)
			pass
		else:
			if self.log_level >= 7:
				print("response:", response)
			if moses_common.is_success(response) and 'Items' in response:
				records = self.convert_from_item(response['Items'])
				if len(records) and records[0] and self._sort_key.name in records[0]:
					return records[0][self._sort_key.name]
		return int(0)
	
	def get_max_limit(self):
		max_limit = 1000
		if 'TableSizeBytes' in self._info and 'ItemCount' in self._info:
			safe_ddb_limit = 1000000 * .75
			avg_record_size = self._info['TableSizeBytes'] / self._info['ItemCount']
			max_limit = int(safe_ddb_limit / avg_record_size)
		return round(max_limit, -2)
	
	"""
	Same args as query() except limit and offset.
	key_condition, attribute_names, attribute_values = table.get_key_condition_expressions(partition_key_value, args=None)
	"""
	def get_key_condition_expressions(self, partition_key_value, args={}):
		if args and type(args) is not dict:
			raise AttributeError("args must be dict")
		
		key_condition = '#pkey = :pvalue'
		attribute_names = {"#pkey":self._partition_key.name}
		attribute_values = {":pvalue":self.convert_to_attribute_value(partition_key_value)}
		
		if 'sort_key_value' in args and self._sort_key:
			sort_key_operator = '='
			if 'sort_key_operator' in args:
				sort_key_operator = args['sort_key_operator']
			attribute_names["#skey"] = self._sort_key.name
			attribute_values[":svalue"] = self.convert_to_attribute_value(args['sort_key_value'])
			
			if sort_key_operator == 'between':
				if 'sort_key_value_end' not in args:
					raise AttributeError("'between' operator requires sort_key_value_end for table '{}' sort key '{}'".format(self._name, self._sort_key.name))
				key_condition += ' AND #skey BETWEEN :svalue AND :svalue2'
				attribute_values[":svalue2"] = self.convert_to_attribute_value(args['sort_key_value_end'])
			elif sort_key_operator == 'begins_with':
				key_condition += ' AND begins_with (#skey, :svalue)'
			else:
				key_condition += ' AND #skey {} :svalue'.format(sort_key_operator)
		return key_condition, attribute_names, attribute_values
	
	"""
	Same args as query() except limit and offset.
	update_expression, attribute_names, attribute_values = table.get_update_expression(partition_key_value, args=None)
	"""
	def get_update_expression(self, item):
		if item and type(item) is not dict:
			raise TypeError("item must be dict")
		
		# Remove keys from item
		if self._partition_key.name in item:
			item.pop(self._partition_key.name)
		if self._sort_key and self._sort_key.name in item:
			item.pop(self._sort_key.name)
		
		count = 0
		expression_list = []
		attribute_names = {}
		attribute_values = {}
		for key, value in item.items():
			count += 1
			attribute_names["#k" + str(count)] = key
			attribute_values[":v" + str(count)] = self.convert_to_attribute_value(value)
			expression = "#k{} = :v{}".format(str(count), str(count))
			expression_list.append(expression)
		
		if len(expression_list) < 1:
			return True, True, True
		update_expression = "SET " + ', '.join(expression_list)
		
		return update_expression, attribute_names, attribute_values
	
	"""
	Same args as query() except limit and offset.
	count = table.query_count(partition_key_value, args=None)
	"""
	def query_count(self, partition_key_value, args={}):
		if args and type(args) is not dict:
			raise AttributeError("args must be dict")
		
		key_condition, attribute_names, attribute_values = self.get_key_condition_expressions(partition_key_value, args)
		
		try:
			response = boto3_client.query(
				TableName = self.name,
				Select = 'COUNT',
				KeyConditionExpression = key_condition,
				ExpressionAttributeNames = attribute_names,
				ExpressionAttributeValues = attribute_values
			)
		
		except ClientError as e:
			print("error:", e)
			raise ConnectionError("Failed to query table '{}'".format(self._name))
		
		else:
			if self.log_level >= 7:
				print("response:", response)
			if moses_common.is_success(response) and 'Count' in response:
				return response['Count']
				
	"""
	records, total = table.query(partition_key_value)
	records, total = table.query(partition_key_value, {
		"sort_key_value": value,
		"sort_key_value_end": value,  # Required by sort_key_operator 'between'
		"sort_key_operator": '='|'<'|'<='|'>'|'>='|'begins_with'|'between',  # defaults to '='
		"limit": int,
		"offset": int
	})
	"""
	def query(self, partition_key_value, args={}):
		if args and type(args) is not dict:
			raise AttributeError("args must be dict")
		
		key_condition, attribute_names, attribute_values = self.get_key_condition_expressions(partition_key_value, args)
		
		limit = None
		if 'limit' in args:
			limit = self.convert_to_int(args['limit'])
			if limit < 1:
				limit = None
		
		offset = None
		if 'offset' in args:
			offset = self.convert_to_int(args['offset']) - 1
			if offset < 1:
				offset = None
			elif limit:
				limit += offset
		
		if self.log_level >= 7:
			print("args {}: {}".format(type(args), args))
			print("self._sort_key.name {}: {}".format(type(self._sort_key.name), self._sort_key.name))
		sort_forward = True
		if 'order_by' in args and type(args['order_by']) is list:
			for element in args['order_by']:
				if 'field' in element and element['field'] == self._sort_key.name:
					if 'order' in element and element['order'] == 'desc':
						sort_forward = False
		
		if not limit:
			limit = self.get_max_limit()
		
		try:
			response = boto3_client.query(
				TableName = self.name,
				Select = 'ALL_ATTRIBUTES',
				KeyConditionExpression = key_condition,
				ExpressionAttributeNames = attribute_names,
				ExpressionAttributeValues = attribute_values,
				ScanIndexForward = sort_forward,
				Limit = limit
			)
		
		except ClientError as e:
			print("error:", e)
			raise ConnectionError("Failed to query table '{}'".format(self._name))
		
		else:
			if self.log_level >= 7:
				print("response:", response)
			if moses_common.is_success(response) and 'Items' in response:
				records = self.convert_from_item(response['Items'])
				
				# Get count
				count = len(records)
				if 'LastEvaluatedKey' in response:
					count = self.query_count(partition_key_value, args)
				
				# If no offset, return all records
				if not offset:
					return records, count
				
				# If offset is too large, return 0 records
				if offset >= len(records):
					return [], count
				
				# Return subset of records
				offset_records = []
				for i in range(offset, len(records)):
					offset_records.append(records[i])
				return offset_records, count
				
	
	"""
	records, total = table.scan()
	"""
	def scan(self):
		response = {}
		try:
			response = boto3_client.scan(
				TableName = self._name
			)
		except ClientError as e:
			print("error:", e)
			raise ConnectionError("Failed to scan DynamodDB", self.name)
		else:
			if self.log_level >= 7:
				print("response:", response)
			if moses_common.is_success(response) and 'Items' in response:
				count = None
				if 'ItemCount' in self._info:
					count = self._info['ItemCount']
				return self.convert_from_item(response['Items']), count
	
	"""
	table.update_item(item)
	"""
	def update_item(self, item):
		if type(item) is not dict:
			raise TypeError("item must be a dict")
			return
		if self._partition_key.name not in item:
			print("Update item is missing partition key for table", self._name)
			return
		
		partition_key_value = item[self._partition_key.name]
		key_object = {
			self._partition_key.name: self.convert_to_attribute_value(item[self._partition_key.name], self._partition_key.type)
		}
		if self._sort_key:
			if self._sort_key.name not in item:
				print("Update item is missing sort key for table", self._name)
				return
			key_object[self._sort_key.name] = self.convert_to_attribute_value(item[self._sort_key.name], self._sort_key.type)
		
		update_expression, attribute_names, attribute_values = self.get_update_expression(item)
		if update_expression and type(update_expression) == type(True):
			return True
		if self.log_level >= 7:
			print("key_object {}: {}".format(type(key_object), key_object))
			print("update_expression {}: {}".format(type(update_expression), update_expression))
			print("attribute_names {}: {}".format(type(attribute_names), attribute_names))
			print("attribute_values {}: {}".format(type(attribute_values), attribute_values))
		
		if self._dry_run:
			self._ui.dry_run("Update item: {}".format(item))
			return True
		try:
			response = boto3_client.update_item(
				TableName = self._name,
				Key = key_object,
				UpdateExpression = update_expression,
				ExpressionAttributeNames = attribute_names,
				ExpressionAttributeValues = attribute_values
			)
		except ClientError as e:
			if self.log_level >= 7:
				print("error:", e)
			raise ConnectionError("Failed to update DynamodDB", self.name)
		else:
			if self.log_level >= 7:
				print("response:", response)
			if moses_common.is_success(response):
				return True
		return False
	
	"""
	table.put_item(item)
	"""
	def put_item(self, item):
		if type(item) is not dict:
			return
		new_item = self.convert_to_item(item)
# 		print("new_item {}: {}".format(type(new_item), new_item))
		if self._dry_run:
			self._ui.dry_run("Put item: {}".format(item))
			return True
		try:
			response = boto3_client.put_item(
				TableName = self._name,
				Item = self.convert_to_item(item)
			)
		except ClientError as e:
			if self.log_level >= 7:
				print("error:", e)
			raise ConnectionError("Failed to put item DynamodDB", self.name)
		else:
			if self.log_level >= 7:
				print("response:", response)
			if moses_common.is_success(response):
				return True
		return False

	"""
	table.delete_item(partition_key_value)
	table.delete_item(partition_key_value, sort_key_value)
	"""
	def delete_item(self, partition_key_value, sort_key_value=None):
		key_hash = {
			self.partition_key.name: partition_key_value
		}
		if sort_key_value:
			key_hash[self.sort_key.name] = sort_key_value
		
		key_hash = self.convert_to_item(key_hash)
		if self._dry_run:
			if sort_key_value:
				self._ui.dry_run(f"Delete item {self.name}.{partition_key_value}.{sort_key_value}")
			else:
				self._ui.dry_run(f"Delete item {self.name}.{partition_key_value}")
			return True
		try:
			response = boto3_client.delete_item(
				TableName = self._name,
				Key = key_hash
			)
		except ClientError as e:
			if self.log_level >= 7:
				print("error:", e)
			raise ConnectionError("Failed to delete item", self.name)
		else:
			if self.log_level >= 7:
				print("response:", response)
			if moses_common.is_success(response):
				return True
		return False




class Index:
	"""
	index = moses_common.dynamodb.Index(table, args)
	"""
	def __init__(self, table, args, log_level=5, dry_run=False):
		self._dry_run = dry_run
		self.log_level = log_level
		
		if not args or type(args) is not dict:
			raise AttributeError("Invalid index args")
		if 'IndexName' not in args:
			raise AttributeError("IndexName is required in args")
		self._name = args['IndexName']
		self._table = table
		self._partition_key = None
		self._sort_key = None
		self._info = None
		if self.load(args):
			self._exists = True
		else:
			self._exists = False
		self._ui = moses_common.ui.Interface()
	
	'''
	{
		"IndexArn": "arn:aws:dynamodb:us-west-2:884259710201:table/reports-export-dev/index/status-create_time",
		"IndexName": "status-create_time",
		"IndexSizeBytes": "8709",
		"IndexStatus": "ACTIVE",
		"ItemCount": "38",
		"KeySchema": [
			{
				"AttributeName": "status",
				"KeyType": "HASH"
			}, {
				"AttributeName": "create_time",
				"KeyType": "RANGE"
			}
		],
		"Projection": { "ProjectionType": "ALL" },
		"ProvisionedThroughput": { "NumberOfDecreasesToday": "0", "ReadCapacityUnits": "1", "WriteCapacityUnits": "1" }
	}
	'''
	def load(self, args):
		self._info = args
		if 'KeySchema' in args and type(args['KeySchema']) is list:
			for i in range(len(args['KeySchema'])):
				key_info = args['KeySchema'][i]
				if key_info and type(key_info) is dict and 'AttributeName' in key_info:
					if i == 0:
						self._partition_key = self._table._attributes[key_info['AttributeName']]
					if i == 1:
						self._sort_key = self._table._attributes[key_info['AttributeName']]
			return True
		return False
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = moses_common.normalize_log_level(value)
	
	@property
	def exists(self):
		return self._exists
	
	@property
	def name(self):
		return self._name
	
	@property
	def arn(self):
		if not self._exists and 'IndexArn' not in self._info:
			return None
		return self._info['IndexArn']
	
	"""
	Same args as query() except limit and offset.
	key_condition, attribute_names, attribute_values = table.get_key_condition_expressions(partition_key_value, args=None)
	"""
	def get_key_condition_expressions(self, partition_key_value, args={}):
		if args and type(args) is not dict:
			raise AttributeError("args must be dict")
		
		key_condition = '#pkey = :pvalue'
		attribute_names = {"#pkey":self._partition_key.name}
		attribute_values = {":pvalue":self._table.convert_to_attribute_value(partition_key_value)}
		
		if 'sort_key_value' in args and self._sort_key:
			sort_key_operator = '='
			if 'sort_key_operator' in args:
				sort_key_operator = args['sort_key_operator']
			attribute_names["#skey"] = self._sort_key.name
			attribute_values[":svalue"] = self._table.convert_to_attribute_value(args['sort_key_value'])
			
			if sort_key_operator == 'between':
				if 'sort_key_value_end' not in args:
					raise AttributeError("'between' operator requires sort_key_value_end for table '{}' sort key '{}'".format(self._name, self._sort_key.name))
				key_condition += ' AND #skey BETWEEN :svalue AND :svalue2'
				attribute_values[":svalue2"] = self._table.convert_to_attribute_value(args['sort_key_value_end'])
			elif sort_key_operator == 'begins_with':
				key_condition += ' AND begins_with (#skey, :svalue)'
			else:
				key_condition += ' AND #skey {} :svalue'.format(sort_key_operator)
		return key_condition, attribute_names, attribute_values
	
	"""
	Same args as query() except limit and offset.
	count = table.query_count(partition_key_value, args=None)
	"""
	def query_count(self, partition_key_value, args={}):
		if args and type(args) is not dict:
			raise AttributeError("args must be dict")
		
		key_condition, attribute_names, attribute_values = self.get_key_condition_expressions(partition_key_value, args)
		
		try:
			response = boto3_client.query(
				TableName = self.name,
				Select = 'COUNT',
				KeyConditionExpression = key_condition,
				ExpressionAttributeNames = attribute_names,
				ExpressionAttributeValues = attribute_values
			)
		
		except ClientError as e:
			print("error:", e)
			raise ConnectionError("Failed to query table '{}'".format(self._name))
		
		else:
			if self.log_level >= 7:
				print("response:", response)
			if moses_common.is_success(response) and 'Count' in response:
				return response['Count']
				
	"""
	records = index.query(partition_key_value)
	records = index.query(partition_key_value, {
		"sort_key_value": value,
		"sort_key_value_end": value,  # Required by sort_key_operator 'between'
		"sort_key_operator": '='|'<'|'<='|'>'|'>='|'begins_with'|'between',  # defaults to '='
		"limit": int,
		"offset": int
	})
	"""
	def query(self, partition_key_value, args={}):
		if args and type(args) is not dict:
			raise AttributeError("args must be dict")
		
		key_condition, attribute_names, attribute_values = self.get_key_condition_expressions(partition_key_value, args)
		
		limit = None
		if 'limit' in args:
			limit = self._table.convert_to_int(args['limit'])
			if limit < 1:
				limit = None
		
		offset = None
		if 'offset' in args:
			offset = self._table.convert_to_int(args['offset']) - 1
			if offset < 1:
				offset = None
			elif limit:
				limit += offset
		
		if not limit:
			limit = self._table.get_max_limit()
		
		try:
			response = boto3_client.query(
				TableName = self._table.name,
				IndexName = self.name,
				Select = 'ALL_ATTRIBUTES',
				KeyConditionExpression = key_condition,
				ExpressionAttributeNames = attribute_names,
				ExpressionAttributeValues = attribute_values,
				Limit = limit
			)
		
		except ClientError as e:
			print("error:", e)
			raise ConnectionError("Failed to query table '{}'".format(self._name))
		
		else:
			if self.log_level >= 7:
				print("response:", response)
			if moses_common.is_success(response) and 'Items' in response:
				records = self._table.convert_from_item(response['Items'])
				
				# Get count
				count = len(records)
				if 'LastEvaluatedKey' in response:
					count = self.query_count(partition_key_value, args)
				
				# If no offset, return all records
				if not offset:
					return records, count
				
				# If offset is too large, return 0 records
				if offset >= len(records):
					return [], count
				
				# Return subset of records
				offset_records = []
				for i in range(offset, len(records)):
					offset_records.append(records[i])
				return offset_records, count
				



class Attribute:
	"""
	attribute = moses_common.dynamodb.Attribute(table, args)
	"""
	def __init__(self, table, args, log_level=5, dry_run=False):
		self._dry_run = dry_run
		self.log_level = log_level
		
		if not args or type(args) is not dict:
			raise AttributeError("Invalid attribute args")
		if 'AttributeName' not in args:
			raise AttributeError("AttributeName is required in args")
		self._name = args['AttributeName']
		self._info = args
		self._exists = True
		self._ui = moses_common.ui.Interface()
	
	'''
	{
		"AttributeName": "create_time",
		"AttributeType": "S"
	}
	'''
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = moses_common.normalize_log_level(value)
	
	@property
	def exists(self):
		return self._exists
	
	@property
	def name(self):
		return self._name
	
	@property
	def type(self):
		if not self._exists and 'AttributeType' not in self._info:
			return None
		return self._info['AttributeType']
	
	@property
	def key_type(self):
		if not self._exists and 'key_type' not in self._info:
			return None
		return self._info['key_type']
	

