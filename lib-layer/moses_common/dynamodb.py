# print("Loaded DynamoDB module")

import boto3
import datetime
import re
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

import moses_common.__init__ as common
import moses_common.ui

boto3_client = boto3.client('dynamodb', region_name="us-west-2")

def is_valid_name(name):
	if type(name) is not str:
		return False
	if not re.match(r'[a-zA-Z0-9_.-]{3,255}$', name):
		return False
	return True

"""
Permissions needed:
	DescribeTable
	GetItem
	Query
	Scan
	DeleteItem
	PutItem
	UpdateItem
"""


class Table:
	"""
	import moses_common.dynamodb
	table = moses_common.dynamodb.Table(table_name)
	table = moses_common.dynamodb.Table(table_name, ui=ui, dry_run=dry_run)
	"""
	def __init__(self, table_name, ui=None, dry_run=False):
		self.dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
		
		if not is_valid_name(table_name):
			raise AttributeError("Invalid table name")
		self.name = table_name
		self._indexes = None
		self.attributes = None
		self.partition_key = None
		self.sort_key = None
		if self.load():
			self.exists = True
		else:
			self.exists = False
		
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
		if common.is_success(response) and 'Table' in response and type(response['Table']) is dict:
			self.info = response['Table']
			if 'AttributeDefinitions' in response['Table'] and type(response['Table']['AttributeDefinitions']) is list:
				self.attributes = {}
				for attribute_info in response['Table']['AttributeDefinitions']:
					if 'AttributeName' in attribute_info:
						self.attributes[attribute_info['AttributeName']] = Attribute(self, attribute_info, ui=self.ui, dry_run=self.dry_run)
				if 'KeySchema' in response['Table'] and type(response['Table']['KeySchema']) is list:
					for i in range(len(response['Table']['KeySchema'])):
						key_info = response['Table']['KeySchema'][i]
						if key_info and type(key_info) is dict:
							if 'AttributeName' in key_info:
								self.attributes[key_info['AttributeName']].info['key_type'] = key_info['KeyType']
						if i == 0:
							self.partition_key = self.attributes[key_info['AttributeName']]
						if i == 1:
							self.sort_key = self.attributes[key_info['AttributeName']]
			if 'GlobalSecondaryIndexes' in response['Table'] and type(response['Table']['GlobalSecondaryIndexes']) is list:
				self._indexes = {}
				for index_info in response['Table']['GlobalSecondaryIndexes']:
					index = Index(self, index_info, ui=self.ui, dry_run=self.dry_run)
					self._indexes[index.name] = index
			return True
		return False
	
	@property
	def arn(self):
		if not self.exists and 'TableArn' not in self.info:
			return None
		return self.info['TableArn']
	
	@property
	def item_count(self):
		return self.info['ItemCount']
	
	@property
	def indexes(self):
		if not self.exists or type(self._indexes) is not dict:
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
			if re.search(r'\.', attribute_value['N']):
				return common.convert_to_float(attribute_value['N'])
			else:
				return common.convert_to_int(attribute_value['N'])
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
			self.partition_key.name: self.convert_to_attribute_value(key_value, self.partition_key.type)
		}
		if self.sort_key:
			if sort_value:
				key_object[self.sort_key.name] = self.convert_to_attribute_value(sort_value, self.sort_key.type)
			else:
				raise AttributeError("Table '{}' with sort key '{}' requires a sort key value".format(self.name, self.sort_key.name))
		try:
			response = boto3_client.get_item(
				TableName = self.name,
				Key = key_object
			)
		except ClientError as e:
			self.ui.debug(f"error: {e}")
# 			raise ConnectionError("Failed to get item from DynamodDB " + self.name)
			pass
		else:
			if common.is_success(response) and 'Item' in response:
				results = self.convert_from_item(response['Item'])
				self.ui.debug("get_item: {}".format(len(results)))
				return results
			self.ui.debug(f"get_item response: {response}")
	
	"""
	max_id = table.get_max_range_value(key_value)
	"""
	def get_max_range_value(self, key_value):
		if not self.sort_key:
			raise AttributeError("Table '{}' does not have a sort key".format(self.name))
		if self.sort_key.key_type != 'RANGE':
			raise AttributeError("Table '{}' sort key '{}' is not of type RANGE".format(self.name, self.sort_key.name))
		key_condition = '#partition_key = :key_value'
		attribute_names = {"#partition_key":self.partition_key.name}
		attribute_values = {":key_value":self.convert_to_attribute_value(key_value)}
		projection = '{},{}'.format(self.partition_key.name, self.sort_key.name)
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
			self.ui.debug(f"error: {e}")
# 			raise ConnectionError("Failed to get item from DynamodDB " + self.name)
			pass
		else:
			self.ui.debug(f"get_max_range_value: {response}")
			if common.is_success(response) and 'Items' in response:
				records = self.convert_from_item(response['Items'])
				if len(records) and records[0] and self.sort_key.name in records[0]:
					return records[0][self.sort_key.name]
		return int(0)
	
	def get_max_limit(self):
		max_limit = 1000
		if 'TableSizeBytes' in self.info and 'ItemCount' in self.info:
			safe_ddb_limit = 1000000 * .75
			avg_record_size = self.info['TableSizeBytes'] / self.info['ItemCount']
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
		attribute_names = {"#pkey":self.partition_key.name}
		attribute_values = {":pvalue":self.convert_to_attribute_value(partition_key_value)}
		
		if 'sort_key_value' in args and self.sort_key:
			sort_key_operator = '='
			if 'sort_key_operator' in args:
				sort_key_operator = args['sort_key_operator']
			attribute_names["#skey"] = self.sort_key.name
			attribute_values[":svalue"] = self.convert_to_attribute_value(args['sort_key_value'])
			
			if sort_key_operator == 'between':
				if 'sort_key_value_end' not in args:
					raise AttributeError("'between' operator requires sort_key_value_end for table '{}' sort key '{}'".format(self.name, self.sort_key.name))
				key_condition += ' AND #skey BETWEEN :svalue AND :svalue2'
				attribute_values[":svalue2"] = self.convert_to_attribute_value(args['sort_key_value_end'])
			elif sort_key_operator == 'begins_with':
				key_condition += ' AND begins_with (#skey, :svalue)'
			else:
				key_condition += ' AND #skey {} :svalue'.format(sort_key_operator)
		return key_condition, attribute_names, attribute_values
	
	"""
	filter_expression, attribute_names, attribute_values = table.get_filter_expression([ {
		"name": field_name,
		"operator": "contains",
		"value": field_value
	} ])
	"""
	def get_filter_expression(self, items=None):
		if type(items) is str:
			items = [{
				"name": self.partition_key.name,
				"operator": "contains",
				"value": items
			}]
		elif type(items) is dict:
			items = [items]
		
		if type(items) is not list:
			return '', {}, {}
		
		count = 0
		expression_list = []
		attribute_names = {}
		attribute_values = {}
		for item in items:
			count += 1
			attribute_names["#k" + str(count)] = item['name']
			attribute_values[":v" + str(count)] = self.convert_to_attribute_value(item['value'])
			operator_function = common.convert_to_snakecase(item['operator'].lower())
			if operator_function in ['begins_with', 'contains']:
				expression = "{} (#k{}, :v{})".format(operator_function, str(count), str(count))
				expression_list.append(expression)
		filter_expression = ''
		if len(expression_list):
			filter_expression = ' and '.join(expression_list)
		
		return filter_expression, attribute_names, attribute_values
	
	"""
	update_expression, attribute_names, attribute_values = table.get_update_expression(dict, remove_keys=None)
	"""
	def get_update_expression(self, item, remove_keys=None):
		if item and type(item) is not dict:
			raise TypeError("item must be dict")
		
		# Remove keys from item
		if self.partition_key.name in item:
			item.pop(self.partition_key.name)
		if self.sort_key and self.sort_key.name in item:
			item.pop(self.sort_key.name)
		
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
		update_expression = ''
		if len(expression_list):
			update_expression = "SET " + ', '.join(expression_list)
		
		if remove_keys:
			remove_list = []
			for key in remove_keys:
				count += 1
				attribute_names["#k" + str(count)] = key
				expression = "#k{}".format(str(count))
				remove_list.append(expression)
			if len(remove_list):
				if update_expression:
					update_expression += ' '
				update_expression += "REMOVE " + ', '.join(remove_list)
		
		if not update_expression:
			return True, True, True
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
			raise ConnectionError("Failed to query table '{}'".format(self.name))
		
		else:
			self.ui.debug(f"query_count: {response}")
			if common.is_success(response) and 'Count' in response:
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
			limit = common.convert_to_int(args['limit'])
			if limit < 1:
				limit = None
		
		offset = None
		if 'offset' in args:
			offset = common.convert_to_int(args['offset']) - 1
			if offset < 1:
				offset = None
			elif limit:
				limit += offset
		
		self.ui.debug("args {}: {}".format(type(args), args))
		self.ui.debug("self.sort_key.name {}: {}".format(type(self.sort_key.name), self.sort_key.name))
		sort_forward = True
		if 'order_by' in args and type(args['order_by']) is list:
			for element in args['order_by']:
				if 'field' in element and element['field'] == self.sort_key.name:
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
			raise ConnectionError("Failed to query table '{}'".format(self.name))
		
		else:
			if common.is_success(response) and 'Items' in response:
				records = self.convert_from_item(response['Items'])
				
				# Get count
				count = len(records)
				if 'LastEvaluatedKey' in response:
					count = self.query_count(partition_key_value, args)
				
				# If no offset, return all records
				if not offset:
					self.ui.debug(f"query no offset: {count}")
					return records, count
				
				# If offset is too large, return 0 records
				if offset >= len(records):
					self.ui.debug(f"query big offset: {count}")
					return [], count
				
				# Return subset of records
				offset_records = []
				for i in range(offset, len(records)):
					offset_records.append(records[i])
				self.ui.debug(f"query with offset: {count}")
				return offset_records, count
			self.ui.debug(f"query: {response}")
				
	
	"""
	records = table.get_keys()
	records = table.get_keys(['extra_fields'])
	"""
	def get_keys(self, included_fields=None):
		response = {}
		items = []
		try:
			projection_expression = "#p"
			expression_attribute_names = {
				"#p": self.partition_key.name
			}
			if self.sort_key:
				projection_expression = "#p, #s"
				expression_attribute_names = {
					"#p": self.partition_key.name,
					"#s": self.sort_key.name
				}
			if included_fields:
				cnt = 0
				for field in included_fields:
					key = f"#f{cnt}"
					projection_expression += f", {key}"
					expression_attribute_names[key] = field
					cnt += 1
			
			response = boto3_client.scan(
				TableName = self.name,
				ProjectionExpression = projection_expression,
				ExpressionAttributeNames = expression_attribute_names
			)
		except ClientError as e:
			print("error:", e)
			raise ConnectionError("Failed to scan DynamodDB", self.name)
		
		self.ui.debug(f"get_keys: {response}")
		if not common.is_success(response) or 'Items' not in response:
			return None
		
		items = response['Items']
		while 'LastEvaluatedKey' in response:
			response = boto3_client.scan(
				TableName = self.name,
				ProjectionExpression = projection_expression,
				ExpressionAttributeNames = expression_attribute_names,
				ExclusiveStartKey=response['LastEvaluatedKey']
			)
			items.extend(response['Items'])
		final_items = self.convert_from_item(items)
		if self.sort_key:
			final_items = sorted(final_items, key=lambda x: x[self.sort_key.name])
		return final_items
	
	"""
	list_of_keys = table.get_keys_as_list()
	"""
	def get_keys_as_list(self):
		full_key_list = self.get_keys()
		key_list = []
		for key_set in full_key_list:
			key_list.append(key_set[self.partition_key.name])
		return key_list
	
	"""
	records = table.scan()
	records = table.scan(partial_string_to_filter_on_partiion_key)
	records = table.scan([{
		"name": field_name,
		"operator": "contains" | "begins_with",
		"value": field_value
	}])
	"""
	def scan(self, filters=None, scan_all=True):
		response = {}
		items = []
		
		filter_expression, attribute_names, attribute_values = self.get_filter_expression(filters)
		
		try:
			if filter_expression:
				response = boto3_client.scan(
					TableName = self.name,
					FilterExpression=filter_expression,
					ExpressionAttributeNames=attribute_names,
					ExpressionAttributeValues=attribute_values
				)
			else:
				response = boto3_client.scan(
					TableName = self.name
				)
		except ClientError as e:
			print("error:", e)
			raise ConnectionError("Failed to scan DynamodDB", self.name)
		
		if not common.is_success(response) or 'Items' not in response:
			self.ui.debug(f"scan {self.name}: no response")
			return None
		
		items = response['Items']
		if scan_all:
			while 'LastEvaluatedKey' in response:
				if filter_expression:
					response = boto3_client.scan(
						TableName = self.name,
						ExclusiveStartKey=response['LastEvaluatedKey'],
						FilterExpression=filter_expression,
						ExpressionAttributeNames=attribute_names,
						ExpressionAttributeValues=attribute_values
					)
				else:
					response = boto3_client.scan(
						TableName = self.name,
						ExclusiveStartKey=response['LastEvaluatedKey']
					)
				items.extend(response['Items'])
		results = self.convert_from_item(items)
		self.ui.debug("scan {}: {}".format(self.name, len(results)))
		return results
	
	
	"""
	table.update_item(item)
	"""
	def update_item(self, item, remove_keys=None):
		if type(item) is not dict:
			raise TypeError("item must be a dict")
			return
		if self.partition_key.name not in item:
			print("Update item is missing partition key for table", self.name)
			return
		
		partition_key_value = item[self.partition_key.name]
		key_object = {
			self.partition_key.name: self.convert_to_attribute_value(item[self.partition_key.name], self.partition_key.type)
		}
		if self.sort_key:
			if self.sort_key.name not in item:
				print("Update item is missing sort key for table", self.name)
				return
			key_object[self.sort_key.name] = self.convert_to_attribute_value(item[self.sort_key.name], self.sort_key.type)
		
		update_expression, attribute_names, attribute_values = self.get_update_expression(item, remove_keys)
		if update_expression and type(update_expression) == type(True):
			return True
		self.ui.debug("key_object {}: {}".format(type(key_object), key_object))
		self.ui.debug("update_expression {}: {}".format(type(update_expression), update_expression))
		self.ui.debug("attribute_names {}: {}".format(type(attribute_names), attribute_names))
		self.ui.debug("attribute_values {}: {}".format(type(attribute_values), attribute_values))
		
		if self.dry_run:
			self.ui.dry_run("Update item: {}".format(item))
			if remove_keys:
				self.ui.dry_run("Remove keys: ['{}']".format("', '".join(remove_keys)))
			return True
		try:
			response = boto3_client.update_item(
				TableName = self.name,
				Key = key_object,
				UpdateExpression = update_expression,
				ExpressionAttributeNames = attribute_names,
				ExpressionAttributeValues = attribute_values
			)
		except ClientError as e:
			self.ui.debug(f"error: {e}")
			raise ConnectionError("Failed to update DynamodDB", self.name)
		else:
			self.ui.debug(f"response: {response}")
			if common.is_success(response):
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
		if self.dry_run:
			self.ui.dry_run("Put item: {}".format(item))
			return True
		try:
			response = boto3_client.put_item(
				TableName = self.name,
				Item = self.convert_to_item(item)
			)
		except ClientError as e:
			self.ui.debug(f"error: {e}")
			raise ConnectionError("Failed to put item DynamodDB", self.name)
		else:
			self.ui.debug(f"response: {response}")
			if common.is_success(response):
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
		if self.dry_run:
			if sort_key_value:
				self.ui.dry_run(f"Delete item {self.name}.{partition_key_value}.{sort_key_value}")
			else:
				self.ui.dry_run(f"Delete item {self.name}.{partition_key_value}")
			return True
		try:
			response = boto3_client.delete_item(
				TableName = self.name,
				Key = key_hash
			)
		except ClientError as e:
			self.ui.debug(f"error: {e}")
			raise ConnectionError("Failed to delete item", self.name)
		else:
			self.ui.debug(f"response: {response}")
			if common.is_success(response):
				return True
		return False




class Index:
	"""
	index = moses_common.dynamodb.Index(table, args)
	index = moses_common.dynamodb.Index(table, args, ui=ui, dry_run=dry_run)
	"""
	def __init__(self, table, args, ui=None, dry_run=False):
		self.dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
		
		if not args or type(args) is not dict:
			raise AttributeError("Invalid index args")
		if 'IndexName' not in args:
			raise AttributeError("IndexName is required in args")
		self.name = args['IndexName']
		self.table = table
		self.partition_key = None
		self.sort_key = None
		self.info = None
		if self.load(args):
			self.exists = True
		else:
			self.exists = False
	
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
		self.info = args
		if 'KeySchema' in args and type(args['KeySchema']) is list:
			for i in range(len(args['KeySchema'])):
				key_info = args['KeySchema'][i]
				if key_info and type(key_info) is dict and 'AttributeName' in key_info:
					if i == 0:
						self.partition_key = self.table.attributes[key_info['AttributeName']]
					if i == 1:
						self.sort_key = self.table.attributes[key_info['AttributeName']]
			return True
		return False
	
	@property
	def arn(self):
		if not self.exists and 'IndexArn' not in self.info:
			return None
		return self.info['IndexArn']
	
	"""
	Same args as query() except limit and offset.
	key_condition, attribute_names, attribute_values = table.get_key_condition_expressions(partition_key_value, args=None)
	"""
	def get_key_condition_expressions(self, partition_key_value, args={}):
		if args and type(args) is not dict:
			raise AttributeError("args must be dict")
		
		key_condition = '#pkey = :pvalue'
		attribute_names = {"#pkey":self.partition_key.name}
		attribute_values = {":pvalue":self.table.convert_to_attribute_value(partition_key_value)}
		
		if 'sort_key_value' in args and self.sort_key:
			sort_key_operator = '='
			if 'sort_key_operator' in args:
				sort_key_operator = args['sort_key_operator']
			attribute_names["#skey"] = self.sort_key.name
			attribute_values[":svalue"] = self.table.convert_to_attribute_value(args['sort_key_value'])
			
			if sort_key_operator == 'between':
				if 'sort_key_value_end' not in args:
					raise AttributeError("'between' operator requires sort_key_value_end for table '{}' sort key '{}'".format(self.name, self.sort_key.name))
				key_condition += ' AND #skey BETWEEN :svalue AND :svalue2'
				attribute_values[":svalue2"] = self.table.convert_to_attribute_value(args['sort_key_value_end'])
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
			raise ConnectionError("Failed to query table '{}'".format(self.name))
		
		else:
			self.ui.debug(f"response: {response}")
			if common.is_success(response) and 'Count' in response:
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
			limit = common.convert_to_int(args['limit'])
			if limit < 1:
				limit = None
		
		offset = None
		if 'offset' in args:
			offset = common.convert_to_int(args['offset']) - 1
			if offset < 1:
				offset = None
			elif limit:
				limit += offset
		
		if not limit:
			limit = self.table.get_max_limit()
		
		try:
			response = boto3_client.query(
				TableName = self.table.name,
				IndexName = self.name,
				Select = 'ALL_ATTRIBUTES',
				KeyConditionExpression = key_condition,
				ExpressionAttributeNames = attribute_names,
				ExpressionAttributeValues = attribute_values,
				Limit = limit
			)
		
		except ClientError as e:
			print("error:", e)
			raise ConnectionError("Failed to query table '{}'".format(self.name))
		
		else:
			self.ui.debug(f"response: {response}")
			if common.is_success(response) and 'Items' in response:
				records = self.table.convert_from_item(response['Items'])
				
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
	attribute = moses_common.dynamodb.Attribute(table, args, ui=ui, dry_run=dry_run)
	"""
	def __init__(self, table, args, ui=None, dry_run=False):
		self.dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
		
		if not args or type(args) is not dict:
			raise AttributeError("Invalid attribute args")
		if 'AttributeName' not in args:
			raise AttributeError("AttributeName is required in args")
		self.name = args['AttributeName']
		self.info = args
		self.exists = True
	
	'''
	{
		"AttributeName": "create_time",
		"AttributeType": "S"
	}
	'''
	
	@property
	def type(self):
		if not self.exists and 'AttributeType' not in self.info:
			return None
		return self.info['AttributeType']
	
	@property
	def key_type(self):
		if not self.exists and 'key_type' not in self.info:
			return None
		return self.info['key_type']
	

