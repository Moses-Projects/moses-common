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

import moses_common.__init__ as common


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
	"""
	def __init__(self, event={}, log_level=5, dry_run=False):
		self._dry_run = dry_run
		self.log_level = log_level
		
		self._event = copy.deepcopy(event)
		if self.log_level >= 7:
			print("event:", self._event)
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
    
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
		if self.headers and name in self.headers:
			return self.headers[name]
	
	@property
	def method(self):
		if 'httpMethod' in self._event:
			return self._event['httpMethod'].upper()
		elif 'requestContext' in self._event and 'httpMethod' in self._event['requestContext']:
			return self._event['requestContext']['httpMethod'].upper()
		elif 'method' in self._event:
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
	def path(self):
		if 'path' in self._event:
			return self._event['path']
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
			return common.convert_value(self._event['body'])
		return None
	
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


