# print("Loaded API Gateway module")

# The api_gateway module interprets API Gateway requests and formats output for the API Gateway
#
#	 Would have used logger if it didn't produce date errors on Lambda
#
# Possible API errors
# 400 (Bad Request)				The client sends some invalid data in the request, for example, missing or incorrect content in the payload or parameters. Could also represent a generic client error.
# 401 (Unauthorized)			The client is not authorized to access the requested resource.
# 403 (Forbidden)				The client is not authenticated.
# 404 (Not Found)				The client is attempting to access a resource that doesn't exist.
# 429 (Too Many Requests)		The client is sending more than the allowed number of requests per unit time.
# 500 (Internal Server Error)	The service failed in an unexpected way.
# 502 (Bad Gateway)				A dependent service is throwing errors.
# 503 (Service Unavailable)		The service is failing but is expected to recover.
# 504 (Gateway Timeout)			A dependent service is timing out.

import base64
import copy
import json
import re
import urllib.request
import urllib.parse
from botocore.exceptions import ClientError
from boto3 import client as boto3_client

import moses_common.__init__ as common
import moses_common.ui


"""
import moses_common.api_gateway

{
	"headers": {
		"Header1": "test",
		"Header2": "test"
	},
	"method": "GET",
	"path": "/path1",
	"query": {
		"limit": 2,
		"offset": 0,
		"id": "1234"
	}
}
{
	"headers": {
		"Header1": "test",
		"Header2": "test"
	},
	"method": "POST",
	"path": "/path1/path2",
	"body": {
		"limit": 2,
		"offset": 0,
		"id": "1234"
	}
}
"""

class Request:
	"""
	api = moses_common.api_gateway.Request()
	api = moses_common.api_gateway.Request(ui=ui, dry_run=dry_run)
	"""
	def __init__(self, event={}, ui=None, dry_run=False):
		self._dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()

		self._event = copy.deepcopy(event)

		self.ui.debug(f"event: {self._event}")
		self.ui.info(f"path: *{self.path}*")
		self.ui.info(f"method: *{self.method}*")
		if self.method == 'GET':
			self.ui.info(f"query: {self.query}")
		else:
			self.ui.info(f"body: {self.body}")
		if self.cookies:
			self.ui.info(f"cookies: {self.cookies}")

	def is_api_gateway(self):
		if 'use_authentication' in self._event and not self._event['use_authentication']:
			return False
		if 'httpMethod' in self._event:
			return True
		return False

	@property
	def headers(self):
		if 'headers' in self._event:
			return self._event['headers']
		return None

	def get_header(self, name):
		if self.headers:
			for header, value in self.headers.items():
				if name.lower() == header.lower():
					return value

	@property
	def method(self):
		if self._event.get('httpMethod'):
			return self._event['httpMethod'].upper()
		elif 'requestContext' in self._event and self._event['requestContext'].get('httpMethod'):
			return self._event['requestContext']['httpMethod'].upper()
		elif 'requestContext' in self._event and 'http' in self._event['requestContext'] and self._event['requestContext']['http'].get('method'):
			return self._event['requestContext']['http']['method'].upper()
		elif self._event.get('method'):
			return self._event['method'].upper()
		return None

	@property
	def authorization(self):
		if 'headers' not in self._event or 'Authorization' not in self._event['headers']:
			return None
		auth_parts = re.split(r' ', self._event['headers']['Authorization'], 1)
		if len(auth_parts) != 2:
			return None

		scheme = auth_parts[0].lower()
		if scheme == 'basic':
			user_pass = base64.b64decode(auth_parts[1])
			if user_pass:
				user_pass_parts = re.split(r':', user_pass.decode('utf-8'), 1)
				if len(user_pass_parts) == 2:
					return {
						'scheme': scheme,
						'username': user_pass_parts[0],
						'password': user_pass_parts[1]
					}
		elif scheme == 'bearer':
			return {
				'scheme': scheme,
				'token': auth_parts[1]
			}
		return None

	@property
	def ip_address(self):
		ip_address = None
		if 'headers' in self._event:
			addresses_string = None
			if 'X-Forwarded-For' in self._event['headers']:
				addresses_string = self._event['headers']['X-Forwarded-For']
			elif 'x-forwarded-for' in self._event['headers']:
				addresses_string = self._event['headers']['x-forwarded-for']
			elif 'requestContext' in self._event and 'identity' in self._event['requestContext'] and 'sourceIp' in self._event['requestContext']['identity']:
				addresses_string = self._event['requestContext']['identity']['sourceIp']
			elif 'requestContext' in self._event and 'http' in self._event['requestContext'] and 'sourceIp' in self._event['requestContext']['http']:
				addresses_string = self._event['requestContext']['http']['sourceIp']

			if type(addresses_string) is not str or not len(addresses_string):
				return None

			addresses = re.compile(r"\s*,\s*").split(addresses_string)
			if addresses[0]:
				return addresses[0]
		return None

	@property
	def host(self):
		if 'headers' in self._event and 'host' in self._event['headers']:
			return self._event['headers']['host']
		elif 'requestContext' in self._event and 'domainName' in self._event['requestContext']:
			return self._event['requestContext']['domainName']
		return None

	@property
	def path(self):
		if 'path' in self._event:
			return self._event['path']
		elif 'rawPath' in self._event:
			return self._event['rawPath']
		elif 'requestContext' in self._event and 'path' in self._event['requestContext'] and 'stage' in self._event['requestContext']:
			stage = self._event['requestContext']['stage']
			path_match = r'/{}'.format(stage)
			return re.sub(path_match, '', self._event['requestContext']['path'])
		return None

	@property
	def full_path(self):
		if 'requestContext' in self._event and 'path' in self._event['requestContext']:
			return self._event['requestContext']['path']
		return None

	@property
	def resource(self):
		if 'resource' in self._event and self._event['resource'] != None:
			return str(self._event['resource'])

	def get_param(self, arg):
		if 'pathParameters' in self._event and self._event['pathParameters']:
			if arg in self._event['pathParameters']:
				return self._event['pathParameters'][arg]

	def parse_path(self):
		path = ''
		if 'path' in self._event:
			path = re.sub(r'^/', '', self._event['path'])
		elif 'requestContext' in self._event and 'path' in self._event['requestContext'] and 'stage' in self._event['requestContext']:
			stage = self._event['requestContext']['stage']
			path_match = r'/{}/'.format(stage)
			path = re.sub(path_match, '', self._event['requestContext']['path'])

		path_parts = re.split(r'/', path)
		return path_parts

	@property
	def input(self):
		input = {}
		if self._event['multiValueQueryStringParameters']:
			input = self._event['multiValueQueryStringParameters']
		if self.body:
			for key, value in self.body.items():
				if key in input:
					input[key].extend(value)
				else:
					input[key] = [value]
		return input

	@property
	def query(self):
		if 'queryStringParameters' in self._event and type(self._event['queryStringParameters']) is dict:
			return copy.deepcopy(self._event['queryStringParameters'])
		if 'query' in self._event and type(self._event['query']) is dict:
			return copy.deepcopy(self._event['query'])
		return None

	def process_query(self):
		query = {}
		metadata = {
			'offset': 0,
			'limit': 20,
			'sort': None,
			'order': None
		}
		query_source = None
		if 'multiValueQueryStringParameters' in self._event and type(self._event['multiValueQueryStringParameters']) is dict:
			query_source = self._event['multiValueQueryStringParameters']
		elif 'query' in self._event and type(self._event['query']) is dict:
			query_source = self._event['query']
		else:
			return query, metadata

		for key, value_list in query_source.items():
			if type(value_list) is not list:
				value_list = [value_list]
			if key == 'offset' or key == 'limit':
				if len(value_list) and value_list[0] and int(value_list[0]) > 0:
					metadata[key] = int(value_list[0])
			elif key == 'sort':
				metadata[key] = str(value_list[0])
			elif key == 'order':
				if value_list[0].lower() == 'asc':
					metadata[key] = 'asc'
				elif value_list[0].lower() == 'desc':
					metadata[key] = 'desc'
			else:
				query[key] = value_list
		return query, metadata

	@property
	def body(self):
		if 'body' in self._event and self._event['body']:
			content_type = self.get_header('Content-Type')
			if content_type and content_type.lower() == 'application/x-www-form-urlencoded':
				return common.url_decode(self._event['body'])
			return common.convert_value(self._event['body'])
		return None

	@property
	def cookies(self):
		if 'headers' in self._event and self._event['headers'].get('Cookie'):
			cookies = common.url_decode(self._event['headers']['Cookie'])
			for key, value in cookies.items():
				new_value = []
				for element in value:
					new_value.append(common.convert_value(element))
				cookies[key] = new_value
			return cookies
		return None

	def get_cookie_string(self,
		key,
		value,
		secure=True,
		http_only=True,
		domain=None,
		path='/',
		expires=None,
		max_age=1,
		host_prefix=True,
		secure_prefix=False,
		delete=False
	):

		if type(value) is dict or type(value) is list:
			value = common.make_base64(common.make_json(value))

		if host_prefix:
			key = '__Host-' + key
		elif secure_prefix:
			key = '__Secure-' + key

		if delete:
			cookie = key + '='
		else:
			cookie = key + '=' + value

		if domain:
			cookie += '; domain=' + domain

		if path:
			cookie += '; path=' + path

		time_format = "%a, %d %b %Y %H:%M:%S %Z"
		if delete:
			dt = common.convert_string_to_datetime('1970-01-01T00:00:00Z')
			cookie += '; expires=' + dt.strftime(time_format)

		elif max_age:
			dt = common.get_dt_future(days=max_age)
			cookie += '; expires=' + dt.strftime(time_format)

		elif expires:
			dt = common.convert_string_to_datetime(expires)
			cookie += '; expires=' + dt.strftime(time_format)

		if secure:
			cookie += '; Secure'

		if http_only:
			cookie += '; HttpOnly'

		return cookie



	"""
	new_event = api.generate_event(stage, path, method='GET', query={}, body={})
	"""
	def generate_event(self, stage, path, method='GET', query={}, body={}):
		event = copy.deepcopy(self._event)

		# Headers
		if 'headers' in event and 'Referer' not in event['headers']:
			event['headers']['Referer'] = "{}://{}".format(event['headers']['X-Forwarded-Proto'], event['headers']['X-Original-Host'])
			event['multiValueHeaders']['Referer'] = [ "{}://{}".format(event['headers']['X-Forwarded-Proto'], event['headers']['X-Original-Host']) ]

		# Method
		event['httpMethod'] = method.upper()
		event['requestContext']['httpMethod'] = event['httpMethod']

		# Path
		event['path'] = path
		path_base = re.sub(r'^/', '', path)
		path_base = re.sub(r'/.*', '', path_base)
		path_param = re.sub(r'^.*?/', '', path)
		event['pathParameters'] = { "proxy": path_param }
		if 'requestContext' not in event:
			event['requestContext'] = {}
		event['requestContext']['path'] = '/{}{}'.format(stage, path)
		event['requestContext']['resourcePath'] = '/{}/{}'.format(path_base, '{proxy+}')
		event['requestContext']['stage'] = stage
		event['resource'] = event['requestContext']['resourcePath']

		# Query string
		event['queryStringParameters'] = {}
		event['multiValueQueryStringParameters'] = {}
		for key, value in query.items():
			if type(value) is list:
				event['queryStringParameters'][key] = value[0]
				event['multiValueQueryStringParameters'][key] = value
			else:
				event['queryStringParameters'][key] = value
				event['multiValueQueryStringParameters'][key] = [value]

		# Body
		event['body'] = None
		if len(body):
			event['body'] = json.dumps(body)

		return event


	"""
	Legacy Methods
	"""


	def get_post_data(self):
		"""
		post_data = api.get_post_data()
		"""
		post_data = None
		if 'body' in self._event and self._event['body'] != None:
			# JSON payload
			if common.is_json(self._event['body']):
				post_data = json.loads(self._event['body'])
			# Key pair payload
			elif re.search(r'=', self._event['body']):
				post_data = {}
				pairs = self._event['body'].split('&');
				for pair in pairs:
					mg = re.match(r'^(.*?)=(.*)$', pair);
					if (mg):
						name = mg.group(1)
						value = re.sub(r'\+', ' ', mg.group(2))
						value = urllib.parse.unquote(value)
						post_data[name] = value
			# Plain text
			else:
				post_data = self._event['body']
		return post_data

	def get_post(self, arg, type=None):
		"""
		input = api.get_post(field_name)
		"""
		post_data = api.get_post_data()
		for name, value in post_data:
			if name == arg:
				if type == 'json':
					value = json.loads(value)
				return value

	def get_query(self, arg=None):
		if 'queryStringParameters' in self._event and type(self._event['queryStringParameters']) is dict:
			if arg:
				if arg in self._event['queryStringParameters']:
					return self._event['queryStringParameters'][arg]
			else:
				return self._event['queryStringParameters']
		return None

	def is_dev_api(self):
		if 'requestContext' in self._event and 'domainPrefix' in self._event['requestContext']:
			if re.match(r'.*\-dev$', self._event['requestContext']['domainPrefix']):
				return True
			else:
				return False
		else:
			return True

	def is_prod_api(self):
		return not self.is_dev_api()

	def format_response(self, output, type='json'):
		"""
		output = api.format_response(output, 'json')
		"""
		content_type = ''
		body = output
		if type == 'jsonpretty':
			content_type = "application/json"
			body = json.dumps(output, sort_keys=True, indent=2)
		elif type == 'json':
			content_type = "application/json"
			body = json.dumps(output)
		elif type == 'text':
			content_type = "text/plain"
		else:
			raise("Invalid type '{}' passed to aws.api_gateway.format_response()".format(type))

		return {
			'isBase64Encoded': False,
			'statusCode': 200,
			'headers': { 'Content-Type': content_type },
			'body': body
		}

	def format_error_response(self, error, message = ""):
		"""
		output = api.format_error_response(error, message)
		"""
		code = 0
		if error.lower() == 'bad request': code = 400
		elif error.lower() == 'unauthorized': code = 401
		elif error.lower() == 'forbidden': code = 403
		elif error.lower() == 'not found': code = 404
		elif error.lower() == 'method not allowed': code = 405
		elif error.lower() == 'too many requests': code = 429
		elif error.lower() == 'internal server error': code = 500
		elif error.lower() == 'bad gateway': code = 502
		elif error.lower() == 'service unavailable': code = 503
		elif error.lower() == 'gateway timeout': code = 504
		else:
			raise("Invalid error '{}' passed to aws.api_gateway.format_error_response()".format(error))

		output = {
			'isBase64Encoded': False,
			'statusCode': code,
			'headers': {
				'Content-Type': "application/json"
			}
		}
		if not message:
			message = error.lower().capitalize()

		output['headers']['x-amzn-ErrorType'] = re.sub(r' +', '', error.title())
		output['body'] = json.dumps({
			'message': message
		})

		return output

	def get_pagination_from_query(self):
		query_string = self.get_query()
		page_size = None
		page_number = None
		order_by = None
		if query_string and type(query_string) is dict:
			if 'page_size' in query_string:
				page_size = int(query_string['page_size'])
				if page_size < 1:
					page_size = None
				if page_size and 'page_number' in query_string:
					page_number = int(query_string['page_number'])
					if page_number < 1:
						page_number = 1
			if 'order_by' in query_string:
				order_parts = query_string['order_by'].split(',')
				order_by = []
				for part in order_parts:
					part_parts = part.split(' ')
					order_by_element = {
						"field": part_parts[0],
						"order": "asc"
					}
					if len(part_parts) > 1 and part_parts[1].lower() == 'desc':
						order_by_element["order"] = "desc"
					order_by.append(order_by_element)
		limit, offset = self.get_limit_offset(page_size, page_number)
		pagination = {}
		if page_size:
			pagination['page_size'] = page_size
			pagination['limit'] = page_size
		if page_number:
			pagination['page_number'] = page_number
		if offset:
			pagination['offset'] = offset
		if order_by:
			pagination['order_by'] = order_by
		return pagination

	def get_limit_offset(self, page_size=None, page_number=1):
		if type(page_size) is int or type(page_size) is str:
			page_size = int(page_size)
			if page_size < 1:
				page_size = 1
		else:
			page_size = None

		if type(page_number) is int or type(page_number) is str:
			page_number = int(page_number)
			if page_number < 1:
				page_number = 1
		else:
			page_number = 1

		limit = page_size
		offset = None
		if page_size:
			offset = int(page_size * (page_number-1)) + 1
		return limit, offset

	def get_pagination_links(self, count=None, pagination={}):
		if count and (type(count) is int or type(count) is str):
			count = int(count)
		if type(count) is None:
			return None
		meta_data = {
			"count": count
		}

		if not pagination or type(pagination) is not dict:
			return meta_data
		page_size = None
		if 'page_size' in pagination and (type(pagination['page_size']) is int or type(pagination['page_size']) is str):
			page_size = int(pagination['page_size'])
		if not page_size or page_size < 1:
			return meta_data
		meta_data['page_size'] = page_size
		# Pagination not needed
		if count < page_size:
			return meta_data

		if 'page_number' in pagination and (type(pagination['page_number']) is int or type(pagination['page_number']) is str):
			page_number = int(pagination['page_number'])
			if page_number < 1:
				page_number = 1
		else:
			page_number = 1
		meta_data['page_number'] = page_number

		order_by = ''
		if 'order_by' in pagination:
			for element in pagination['order_by']:
				order = 'asc'
				if 'order' in element and element['order'] == 'desc':
					order = 'desc'
				if 'field' in element:
					if len(order_by):
						order_by += ','
					else:
						order_by = '&'
					order_by += "{}%20{}".format(element['field'], order)

		path = self.path
		path += '?'

		# First page
		meta_data['pagination_links'] = {
			"first_page": "{}page_size={}&page_number=1{}".format(path, page_size, order_by)
		}

		# Last page
		last_page = int(count / page_size)
		if count % page_size:
			last_page += 1
		meta_data['last_page_number'] = last_page
		if last_page > 1:
			meta_data['pagination_links']['last_page'] = "{}page_size={}&page_number={}{}".format(path, page_size, last_page, order_by)

		# Prev page
		if page_number > 1:
			prev_page = page_number - 1
			if prev_page > last_page:
				prev_page = last_page
			meta_data['pagination_links']['prev_page'] = "{}page_size={}&page_number={}{}".format(path, page_size, prev_page, order_by)

		# Next page
		if last_page >= (page_number + 1):
			meta_data['pagination_links']['next_page'] = "{}page_size={}&page_number={}{}".format(path, page_size, page_number + 1, order_by)

		return meta_data


class API:
	"""
	import moses_common.api_gateway
	api = moses_common.api_gateway.API(api_name, ui=ui, dry_run=dry_run)
	"""
	def __init__(self, api_name, ui=None, dry_run=False):
		self.dry_run = dry_run

		self.ui = ui or moses_common.ui.Interface()
		self.client = boto3_client('apigateway', region_name="us-west-2")

		self.name = api_name
		self._resources = None
		if self.load():
			self.exists = True
		else:
			self.exists = False

	"""
	api_info = api.load()
	
	{
		"id": "mlgl4rez63",
		"name": "cg-api-dev",
		"description": "ClassGather dev environment API",
		"createdDate": datetime.datetime(2022, 10, 4, 13, 2, 5, tzinfo=tzlocal()),
		"binaryMediaTypes": [ "*/*" ],
		"apiKeySource": "HEADER",
		"endpointConfiguration": {
			"types": [ "REGIONAL" ]
		},
		"disableExecuteApiEndpoint": false
	}
	"""
	def load(self, paginator_value=None):
		paginator_field = 'position'
		list_field = 'items'
		name_field = 'name'
		try:
			if paginator_value:
				response = self.client.get_rest_apis(
					position = paginator_value
				)
			else:
				response = self.client.get_rest_apis()
		except ClientError as e:
			raise e
		else:
			if common.is_success(response) and type(response) is dict and list_field in response:
				for item in response[list_field]:
					if item[name_field] == self.name:
						self._info = item
						return True
				# Grab more results
				if paginator_field in response:
					return self.load(response[paginator_field])
			return False

	@property
	def id(self):
		if self.exists or 'id' in self._info:
			return self._info['id']
		return None

	@property
	def resources(self):
		if not self._resources:
			self.load_resources()
		return self._resources

	def load_resources(self, paginator_value=None):
		paginator_field = 'position'
		list_field = 'items'
		name_field = 'path'
		try:
			if paginator_value:
				response = self.client.get_resources(
					restApiId = self.id,
					embed = [ 'methods' ],
					position = paginator_value
				)
			else:
				response = self.client.get_resources(
					restApiId = self.id,
					embed = [ 'methods' ]
				)
		except ClientError as e:
			raise e
		else:
			if common.is_success(response) and type(response) is dict and list_field in response:
				if not self._resources:
					self._resources = []
				self._resources.extend(response[list_field])
				# Grab more results
				if paginator_field in response:
					return self.load_resources(response[paginator_field])
				else:
					return True
			return False

	"""
	parent_path, path_part = api.get_path_parts(path)
	parent_path, path_part = api.get_path_parts(path, is_proxy=True)
	"""
	def get_path_parts(self, path, is_proxy=False):
		if is_proxy:
			return path, '{proxy+}'
		parent_path = re.sub(r'\/.*?$', '', path)
		if not parent_path:
			parent_path = '/'
		path_part = re.sub(r'.*\/', '', path)
		return parent_path, path_part


	"""
	resource = api.get_resource(path)
	
	resource = api.get_resource('/')
	{
		"id": "4rcomuaxea",
		"path": "/"
	}
	resource = api.get_resource('/customer')
	{
		"id": "6y3d5d",
		"parentId": "4rcomuaxea",
		"pathPart": "customers",
		"path": "/customers",
		"resourceMethods": {
			"ANY": {}
		}
	}
	resource = api.get_resource('/customer', is_proxy=True)
	{
		"id": "4k3t7t",
		"parentId": "6y3d5d",
		"pathPart": "{proxy+}",
		"path": "/customers/{proxy+}",
		"resourceMethods": {
			"ANY": {}
		}
	}
	"""
	def get_resource(self, path, is_proxy=False, get_original=False):
		if is_proxy:
			path += '/{proxy+}'
		for resource in self.resources:
			if path == resource['path']:
				if get_original:
					return resource
				else:
					return resource.copy()

	"""
	success = api.create_resource(path)
	success = api.create_resource(path, is_proxy=True)
	"""
	def create_resource(self, path, is_proxy=False):
		parent_path, path_part = self.get_path_parts(path, is_proxy=is_proxy)
		parent_resource = self.get_resource(parent_path)
		if not parent_resource:
			return False
		parent_id = parent_resource['id']

		if self.dry_run:
			self.ui.dry_run(f"create_resource('{path}')")
			if self._resources:
				self._resources.append({
					"id": "xxx",
					"parentId": "parent_id",
					"pathPart": path_part,
					"path": path
				})
			return True
		try:
			response = self.client.create_resource(
				restApiId = self.id,
				parentId = parent_id,
				pathPart = path_part
			)
		except ClientError as e:
			raise e
		else:
			if common.is_success(response) and 'path' in response:
				if self._resources:
					self._resources.append(response)
				return True
			return False

	"""
	success = api.get_methods(path)
	success = api.get_methods(path, is_proxy=True)
	"""
	def get_methods(self, path, is_proxy=False):
		if is_proxy:
			path += '/{proxy+}'
		resource = self.get_resource(path)
		if 'resourceMethods' in resource:
			return resource['resourceMethods']
		return []

	"""
	success = api.get_method(path, method)
	success = api.get_method(path, method, is_proxy=True)
	"""
	def get_method(self, path, method, is_proxy=False):
		methods = self.get_methods(path, is_proxy=is_proxy)
		if method in methods:
			return methods[method]
		return None

	"""
	success = api.set_method(path, method, method_def)
	success = api.set_method(path, method, method_def, is_proxy=True)
	"""
	def set_method(self, path, method, method_def, is_proxy=False):
		resource = self.get_resource(path, is_proxy=is_proxy, get_original=True)
		if not resource:
			return False
		if 'resourceMethods' not in resource:
			resource['resourceMethods'] = {}

		resource['resourceMethods'][method.upper()] = method_def
		return True

	"""
	success = api.put_method(path, method)
	success = api.put_method(path, method, is_proxy=True)
	"""
	def put_method(self, path, method, lambda_arn, is_proxy=False):
		resource = self.get_resource(path, is_proxy=is_proxy)
		if not resource:
			return False
		method = method.upper()

		request_parameters = {}
		if is_proxy:
			request_parameters = { "method.request.path.proxy": True }

		new_method_def = None

		if self.dry_run:
			self.ui.dry_run(f"put_method('{path}', '{method}')")
			return True
		try:
			response = self.client.put_method(
				restApiId = self.id,
				resourceId = resource['id'],
				httpMethod = method,
				authorizationType = 'NONE',
				apiKeyRequired = False,
				requestParameters = request_parameters
			)
		except ClientError as e:
			raise e
		else:
			if common.is_success(response) and 'httpMethod' in response:
				del(response['ResponseMetadata'])
				new_method_def = response
			else:
				return False

		if not is_proxy:
			try:
				response = self.client.put_method_response(
					restApiId = self.id,
					resourceId = resource['id'],
					httpMethod = method,
					statusCode = '200',
					responseModels = { "application/json": "Empty" }
				)
			except ClientError as e:
				raise e
			else:
				if common.is_success(response) and 'statusCode' in response:
					del(response['ResponseMetadata'])
					new_method_def['methodResponses'] = {
						response['statusCode']: response
					}
				else:
					return False

		cache_key_parameters = []
		if is_proxy:
			cache_key_parameters = [ "method.request.path.proxy" ]

		try:
			response = self.client.put_integration(
				restApiId = self.id,
				resourceId = resource['id'],
				httpMethod = method,
				type = 'AWS_PROXY',
				integrationHttpMethod = 'POST',
				uri = f"arn:aws:apigateway:us-west-2:lambda:path/2015-03-31/functions/{lambda_arn}/invocations",
				passthroughBehavior = 'WHEN_NO_MATCH',
				contentHandling = 'CONVERT_TO_TEXT',
				cacheNamespace = resource['id'],
				cacheKeyParameters = cache_key_parameters,
				timeoutInMillis = 29000
			)
		except ClientError as e:
			raise e
		else:
			if common.is_success(response) and 'type' in response:
				del(response['ResponseMetadata'])
				new_method_def['methodIntegration'] = response
			else:
				return False

		try:
			response = self.client.put_integration_response(
				restApiId = self.id,
				resourceId = resource['id'],
				httpMethod = method,
				statusCode = '200',
				responseTemplates = {}
			)
		except ClientError as e:
			raise e
		else:
			if common.is_success(response) and 'statusCode' in response:
				del(response['ResponseMetadata'])
				new_method_def['methodIntegration']['integrationResponses'] = {
					response['statusCode']: response
				}
			else:
				return False

		self.set_method(path, method, new_method_def, is_proxy=is_proxy)
		return True
