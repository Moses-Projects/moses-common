# print("Loaded Parameters Store module")

import json
import re
import yaml
from botocore.exceptions import ClientError
from boto3 import client as boto3_client

import moses_common.__init__ as common
import moses_common.ui

class Store:
	"""
	import moses_common.parameter_store
	store = moses_common.parameter_store.Store()
	store = moses_common.parameter_store.Store(ui=ui, dry_run=dry_run)
	"""
	def __init__(self, ui=None, dry_run=False):
		self.dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
		self.client = boto3_client('ssm', region_name="us-west-2")

	"""
	param_list = store.list(param_name_prefix)
	"""
	def list(self, prefix):
		param_list = []
		next_token = None
		while True:
			try:
				if next_token:
					response = self.client.get_parameters_by_path(
						Path=prefix,
						Recursive=True,
						WithDecryption=True,
						NextToken=next_token
					)
				else:
					response = self.client.get_parameters_by_path(
						Path=prefix,
						Recursive=True,
						WithDecryption=True
					)
			except ClientError as e:
				if e.response['Error']['Code'] == 'ParameterNotFound':
					# We can't find the resource that you asked for.
					return False
				raise e
			else:
				# print("response {}: {}".format(type(response), response))
				if common.is_success(response) and type(response) is dict and 'Parameters' in response and type(response['Parameters']) is list:
					param_list.extend(response['Parameters'])
				if response.get('NextToken'):
					next_token = response['NextToken']
				else:
					break
		return param_list

class Param:
	"""
	import moses_common.parameter_store
	param = moses_common.parameter_store.Param(param_name)
	param = moses_common.parameter_store.Param(param_name, ui=ui, dry_run=dry_run)
	"""
	def __init__(self, param_name, ui=None, dry_run=False):
		self.dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
		self.client = boto3_client('ssm', region_name="us-west-2")
		self.name = param_name
		self.info = self.load()
		self._description = None
		self._tags = None
		if self.info and type(self.info) is dict:
			self.exists = True
		else:
			self.exists = False

	"""
	param_info = param.load()
	"""
	def load(self):
		try:
			response = self.client.get_parameter(
				Name = self.name,
				WithDecryption = True
			)
		except ClientError as e:
			if e.response['Error']['Code'] == 'ParameterNotFound':
				# We can't find the resource that you asked for.
				return None
			raise e
		else:
# 			print("response {}: {}".format(type(response), response))
			if common.is_success(response) and type(response) is dict and 'Parameter' in response and type(response['Parameter']) is dict and 'Name' in response['Parameter']:
				return response['Parameter']
			return None

	@property
	def description(self):
		if not self._description:
			try:
				response = self.client.describe_parameters(
					ParameterFilters=[
						{
							'Key': 'Name',
							'Values': [self.name]
						}
					]
				)
			except ClientError as e:
				raise e
			else:
				# 			print("response {}: {}".format(type(response), response))
				if common.is_success(response) and type(response) is dict and 'Parameters' in response and len(response['Parameters']) > 0 and type(
						response['Parameters'][0]) is dict and 'Name' in response['Parameters'][0]:
					self._description = response['Parameters'][0]
		return self._description

	@property
	def tags(self):
		if not self._tags:
			try:
				response = self.client.list_tags_for_resource(
					ResourceType = 'Parameter',
					ResourceId = self.name
				)
			except ClientError as e:
				raise e
			else:
				# print("response {}: {}".format(type(response), response))
				if common.is_success(response) and type(response) is dict and 'TagList' in response:
					self._tags = common.convert_list_to_dict(response['TagList'])
		return self._tags

	def get_arn(self):
		if not self.exists:
			return False
		return self.info['ARN']

	def get_name(self):
		if not self.exists:
			return False
		return self.info['Name']

	"""
	param_value = param.get_value()
	"""
	def get_value(self):
		if not self.exists:
			return None
		value = self.info['Value']
# 		print("response {}: {}".format(type(response), response))
		return common.convert_value(value)

	"""
	required_tier = param.get_tier_for_value(value)
	"""
	def get_tier_for_value(self, value):
		value_length = 0
		if type(value) is str:
			value_length = len(value)
		elif type(value) is dict or type(value) is list:
			value_length = len(json.dumps(value))
		else:
			raise TypeError("value must be str, dict, or list")

		if value_length <= 4096:
			return 'standard'
		if value_length <= 8192:
			return 'advanced'
		raise ValueError("value is too large, {} bytes; value has a max value of 8192 bytes".format(value_length))

	"""
	response = param.create({
		"description": description_string,
		"value": value_dict,
		"tags": tags_dict
	})
	"""
	def create(self, args):
		if not args or type(args) is not dict:
			raise AttributeError("Received invalid args.")

		param_string = ''
		if 'value' in args:
			param_string = args['value']
			if type(args['value']) is dict or type(args['value']) is list:
				if re.search(r'\.yml$', self.name):
					param_string = '---\n' + yaml.dump(args['value'])
				elif re.search(r'\.json$', self.name):
					param_string = json.dumps(args['value'])

		tags_list = []
		if 'tags' in args:
			tags_list = common.convert_tags(args['tags'], 'upper')

		description = ''
		if 'description' in args:
			description = args['description']

		if self.dry_run:
			self.ui.dry_run(f"create() put_parameter('{self.name}', '{description}, '{param_string}', '{tags_list}')")
			self.exists = True
			return True

		response = self.client.put_parameter(
			Name = self.name,
			Description = description,
			Value = param_string,
			Type = 'SecureString',
			Tags = tags_list,
			DataType = 'text',
			Tier = 'Intelligent-Tiering'
		)

# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response):
			self.info = self.load()
			if self.info and type(self.info) is dict:
				self.exists = True
				return self.get_arn()
		return False


	"""
	response = param.modify_value(param_value)
	"""
	def modify_value(self, param_value, format=None):
		if not param_value or (type(param_value) is not dict and type(param_value) is not list):
			raise AttributeError("Received invalid param_value.")

		param_string = ''
		param_string = param_value
		if type(param_value) is dict or type(param_value) is list:
			if format and format == 'json':
				param_string = common.make_json(param_value)
			elif format and format == 'xml':
				param_string = common.make_xml(param_value)
			elif format and format == 'yaml':
				param_string = common.make_yaml(param_value)
			elif re.search(r'\.json$', self.name):
				param_string = common.make_json(param_value)
			elif re.search(r'\.xml$', self.name):
				param_string = common.make_xml(param_value)
			elif re.search(r'\.ya?ml$', self.name):
				param_string = common.make_yaml(param_value)

		if self.dry_run:
			self.ui.dry_run(f"modify_value() put_parameter('{self.name}', '{param_string}')")
			self.exists = True
			return True

		response = self.client.put_parameter(
			Name = self.name,
			Overwrite = True,
			Value = param_string,
			Type = 'SecureString',
			DataType = 'text',
			Tier = 'Intelligent-Tiering'
		)

# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response):
			self.info = self.load()
			if self.info and type(self.info) is dict:
				self.exists = True
				return self.get_arn()
		return False


	"""
	response = param.delete()
	"""
	def delete(self):
		if self.dry_run:
			self.ui.dry_run(f"delete() delete_parameter('{self.name}')")
			self.info = False
			self.exists = False
			return True

		response = self.client.delete_parameter(
			Name = self.name,
		)
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response):
			self.info = False
			self.exists = False
			return True
		return False
