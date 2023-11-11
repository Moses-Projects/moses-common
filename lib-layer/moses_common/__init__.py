# print("Loaded APIs init")

import base64
import csv
import datetime
import json
import math
import os
import re
import requests
import sys
import unidecode
import urllib.request
import urllib.parse
import xmltodict
import yaml

"""
import moses_common.__init__ as common
"""


## HTTP requests

"""
response_code, response_data = common.get_url(url, args)
response_code, response_data = common.get_url(url, {
	"bearer_token": bearer_token
})
response_code, response_data = common.get_url(url, {
	"username": username,
	"password": password
})
response_code, response_data = common.get_url(url, {
	"data": string
})
response_code, response_data = common.get_url(url, {
	"data": {
		"key": "value",
		"key2": "value2",
		...
	}
})
"""
def get_url(url, args={}, debug=False, dry_run=False):
	if type(args) is not dict:
		raise AttributeError("get_url arg should be a dict.")
	
	headers = {}
	if 'headers' in args and type(args['headers']) is dict:
		headers = args['headers']
	
	if 'bearer_token' in args:
		headers['Authorization'] = 'Bearer {}'.format(args['bearer_token'])
	elif 'username' in args and 'password' in args:
		credentials = ('%s:%s' % (args['username'], args['password']))
		encoded_credentials = base64.b64encode(credentials.encode('ascii'))
		headers['Authorization'] = 'Basic {}'.format(encoded_credentials.decode("ascii"))
	
	data = None
	if 'data' in args:
		if type(args['data']) is dict:
			data = urllib.parse.urlencode(args['data'])
			data = data.encode('ascii')
		elif type(args['data']) is str:
			data = args['data'].encode('ascii')
	
	method = 'GET'
	if 'method' in args:
		method = args['method']
	elif data:
		method = 'POST'
	
	if 'query' in args and type(args['query']) is dict:
		query = urllib.parse.urlencode(args['query'])
		url += '?' + query
	
	if debug or dry_run:
		print("method:", method)
		print("url:", url)
		print("headers: {}".format(headers))
		if data:
			print("data {}: {}".format(type(data), data))
	
	if dry_run:
		return 200, "Dry run response"
	
	req = urllib.request.Request(url, data=data, headers=headers, method=method)
	
	try:
		with urllib.request.urlopen(req) as response:
			response = response.read()
	except urllib.error.HTTPError as e:
		return e.code, e.reason
	except urllib.error.URLError as e:
		print("URLError reason: {}".format(e.reason))
		raise e
	except:
		print("Unexpected error:", sys.exc_info()[0])
		raise
	else:
		return 200, convert_value(response.decode('UTF-8'))

def download_url(url, filepath):
	# Download and save the image to a file
	data = requests.get(url).content
	if not data:
		return False
	with open(filepath, "wb") as handler:
		handler.write(data)
	return True

"""
token = common.get_oauth2_token(url, key, secret)
"""
def get_oauth2_token(url, key, secret):
	response_code, response_data = get_url(url, {
		"method": "POST",
		"username": key,
		"password": secret,
		"headers": { "Accept": "application/json" }
	})
	if 'access_token' not in response_data:
		return None
	return response_data['access_token']

def url_encode(hash):
	return urllib.parse.urlencode(hash)

## File formats

"""
boolean = common.is_base64(base64_string)
"""
def is_base64(base64_string):
	base64_object = parse_base64(base64_string)
	if base64_object is None:
		return False
	return True

"""
base64_object = common.parse_base64(base64_string)
"""
def parse_base64(base64_string):
	if type(base64_string) is not str:
		return None
	base64_string = base64_string.lstrip().rstrip()
	if not re.match(r'[A-Za-z0-9\+/]+={0,2}$', base64_string):
		return None
	try:
		base64_object = base64.b64decode(base64_string).decode('utf-8')
		return base64_object
	except ValueError as e:
		return base64_string
	return base64_string


"""
boolean = common.is_json(json_string)
"""
def is_json(json_string):
	json_object = parse_json(json_string)
	if json_object is None:
		return False
	return True

"""
json_object = common.parse_json(json_string)
"""
def parse_json(json_string):
	if type(json_string) is not str:
		return None
	json_string = json_string.lstrip().rstrip()
	if not re.match(r'(\{|\[)', json_string):
		return None
	try:
		# Remove invalid single quote escapes
		json_string = re.sub(r'(?<!\\)\\([^u"])', r'\1', json_string)
		json_object = json.loads(json_string)
	except ValueError as e:
		print(e)
		return None
	return json_object

"""
json_string = common.make_json(python_object)
"""
def make_json(python_object, pretty_print=False):
	python_object = convert_datetime_to_string(python_object)
	if pretty_print:
		return json.dumps(python_object, sort_keys=False, indent=2)
	return json.dumps(python_object)


"""
boolean = common.is_xml(xml_string)
"""
def is_xml(xml_string):
	xml_object = parse_xml(xml_string)
	if xml_object is None:
		return False
	return True

"""
xml_object = common.parse_xml(xml_string)
"""
def parse_xml(xml_string):
	if type(xml_string) is not str:
		return None
	xml_string = xml_string.lstrip().rstrip()
	if not re.match(r'<', xml_string):
		return None
	try:
		xml_object = xmltodict.parse(xml_string)
	except ValueError as e:
		return None
	return xml_object

"""
xml_string = common.make_xml(python_object)
"""
def make_xml(python_object):
	python_object = convert_datetime_to_string(python_object)
	return xmltodict.unparse(python_object)


"""
boolean = common.is_yaml(yaml_string)
"""
def is_yaml(yaml_string):
	yaml_object = parse_yaml(yaml_string)
	if yaml_object is None:
		return False
	return True

"""
yaml_object = common.parse_yaml(yaml_string)
"""
def parse_yaml(yaml_string):
	if type(yaml_string) is not str:
		return None
	yaml_string = yaml_string.lstrip().rstrip()
	if not re.match(r'---', yaml_string):
		return None
	try:
		yaml_object = yaml.load(yaml_string, Loader=yaml.FullLoader)
	except ValueError as e:
		return None
	return yaml_object

"""
yaml_string = common.make_yaml(python_object)
"""
def make_yaml(python_object, pretty_print=False):
	python_object = convert_datetime_to_string(python_object)
	return yaml.dump(python_object)


"""
value_object = common.convert_value(value_string)
  Recognizes and converts Base64, JSON, XML, and YAML into a Python object.
"""
def convert_value(input_string):
	if type(input_string) is not str:
		return input_string
# 	print("input_string {}: {}".format(type(input_string), input_string))
	value_string = parse_base64(input_string)
	if value_string is None:
		value_string = input_string
	
	value_object = parse_json(value_string)
	if value_object is not None:
		return value_object
	value_object = parse_xml(value_string)
	if value_object is not None:
		return value_object
	value_object = parse_yaml(value_string)
	if value_object is not None:
		return value_object
	return value_string
	
"""
object = common.yaml_load(yaml_string)
"""
def yaml_load(yaml_string):
	if type(yaml_string) is not str:
		return yaml_string
	lines = yaml_string.splitlines()
	yaml_dict = {}
	for line in lines:
		if not re.search(r":", line):
			continue
		parts = re.match(r"\s*(\w+)\s*:\s*(.*?)\s*$", line)
		if parts[0]:
			value = parts[2]
			if re.match(r"'.*'$", parts[2]):
				value = re.sub(r"(^'|'$)", '', parts[2], count=0)
			elif re.match(r'".*"$', parts[2]):
				value = re.sub(r'(^"|"$)', '', parts[2], count=0)
			yaml_dict[parts[1]] = value
	return yaml_dict


## File handling

"""
age_in_days = common.get_file_age(filename)
"""
def get_file_age(filepath, file_checked=False):
	if not file_checked:
		filepath = os.path.expanduser(filepath)
		if not os.path.isfile(filepath):
			return None
	epoch = get_epoch()
	return (epoch - os.path.getmtime(filepath)) / 86400
	
"""
data = common.read_file(filename)
For CSVs:
data = common.read_file(filename, delimiter=',', mapping=dict)
"""
def read_file(filepath, delimiter=None, mapping=None, file_checked=False, convert=True):
	if not file_checked:
		filepath = os.path.expanduser(filepath)
		if not os.path.isfile(filepath):
			return None
	if re.search(r'\.csv$', filepath, re.IGNORECASE) and not convert:
		return read_csv(filepath, delimiter=delimiter, mapping=mapping)
	else:
		file = open(filepath, 'r')
		contents = file.read()
		file.close()
		if convert:
			contents = convert_value(contents)
			if mapping and type(mapping) is dict and type(contents) in [dict, list]:
				return map_csv(contents, mapping)
		return contents

"""
success = common.write_file(filename, data)
success = common.write_file(filename, data, format='json')
"""
def write_file(filepath, data, format=None, make_dir=False):
	text = str(data)
	if type(data) is list or type(data) is dict:
		if format == 'json' or re.search(r'\.json$', filepath):
			text = make_json(data)
		elif format == 'xml' or re.search(r'\.xml$', filepath):
			text = make_xml(data)
		elif format == 'yaml' or re.search(r'\.ya?ml$', filepath):
			text = '---\n' + make_yaml(data)
		else:
			return False
	filepath = os.path.expanduser(filepath)
	with open(filepath, "w") as file:
		file.write(text)
		return True

"""
data = common.read_cache(filename, days_to_expire)
"""
def read_cache(filepath, days):
	filepath = os.path.expanduser(filepath)
	if not os.path.isfile(filepath):
		return None
	age = get_file_age(filepath, file_checked=True)
	print("age {}: {}".format(type(age), age))
	if convert_to_float(days) < get_file_age(filepath, file_checked=True):
		return None
	
	return read_file(filepath, file_checked=True)

def _get_default_config_filename():
	if 'HOME' in os.environ:
		filename = re.sub(r'\.\w+$', '', os.path.basename(sys.modules['__main__'].__file__))
		return '{}/.{}.cfg'.format(os.environ['HOME'], filename)
	filename = re.sub(r'\.\w+$', '', sys.modules['__main__'].__file__)
	return '{}.cfg'.format(filename)

"""
settings = common.read_config()
settings = common.read_config(filename)
"""
def read_config(filename=None):
	if not filename:
		filename = _get_default_config_filename()
	filename = os.path.expanduser(filename)
	
	if not os.path.isfile(filename):
		return {}
	
	return read_file(filename, file_checked=True)

"""
common.save_config(data)
common.save_config(data, filename)
"""
def save_config(data, filename=None):
	if not filename:
		filename = _get_default_config_filename()
	filename = os.path.expanduser(filename)
	
	# Read previous settings
	settings = read_config(filename=filename)
	
	# Merge new settings
	for key, value in data.items():
		settings[key] = value
	
	# Save settings
	write_file(filename, settings, format='yaml')
	return filename

"""
records = common.read_csv(filepath)
records = common.read_csv(filepath, delimiter='|')
"""
def read_csv(filepath, delimiter=',', mapping=None):
	filepath = os.path.expanduser(filepath)
	if not os.path.isfile(filepath):
		return None
	records = []
	with open(filepath) as csv_file:
		csv_read = csv.reader(csv_file, delimiter=delimiter)
		
		headers = []
		cnt = 0
		for item in csv_read:
			if cnt == 0:
				for field in item:
					headers.append(convert_to_snakecase(field))
			else:
				record = {}
				for i in range(len(headers)):
					if len(item) >= i:
						record[headers[i]] = item[i]
				records.append(record)
			cnt += 1;
	if mapping and type(mapping) is dict:
		return map_csv(records, mapping)
	return records

"""
new_records = common.map_csv(records, {
	"field": "csv_field_name",
	"field": { "name": "csv_field_name" },
	"field": { "name": "csv_field_name", "type": "str", "transforms": ['lower', 'remove_extra_spaces'] },
	"field": { "name": "csv_field_name", "type": "int", "multiplier": 100 },
	"field": { "name": "csv_field_name", "type": "float" },
	"field": { "name": "csv_field_name", "type": "bool", "true": ['yes', 'y'] },
	"field": { "type": "literal", "value": "value_of_any_type" }
})
"""
def map_csv(records, mapping):
	new_records = []
	for record in records:
		new_record = _map_csv_record(record, mapping)
		if new_record:
			new_records.append(new_record)
	return new_records

_map_csv_last_record = {}
def _map_csv_record(record, mapping):
	if type(record) is not dict:
		raise TypeError("_map_csv_record() record must be type dict. It is {}.".format(type(record)))
	global _map_csv_last_record
	
	new_record = {}
	for key, definition in mapping.items():
		if type(definition) is str:
			definition = { "name": definition }
		elif type(definition) is not dict:
			continue
		
		if 'type' in definition and definition['type'] == 'dict':
			if 'mapping' not in definition or type(definition['mapping']) is not dict:
				continue
			new_record[key] = _map_csv_record(record, definition['mapping'])
			continue
		elif 'type' in definition and definition['type'] == 'literal':
			if 'value' in definition:
				new_record[key] = definition['value']
				continue
		elif 'name' not in definition:
			continue
		elif definition['name'] not in record:
			continue
		
		value = record[definition['name']]
		field_type = 'str'
		if 'type' in definition and definition['type'] in ['str', 'int', 'float', 'bool', 'datetime', 'date', 'time']:
			field_type = definition['type']
		
		if not value:
			if 'carry' in definition and definition['carry'] and key in _map_csv_last_record:
				new_record[key] = _map_csv_last_record[key]
		elif field_type == 'str':
			value = str(value)
			value = value.strip()
			if 'concat' in definition and definition['concat'] in record and record[definition['concat']]:
				if 'delimiter' in definition:
					value += definition['delimiter']
				value += record[definition['concat']].strip()
			if 'transforms' in definition and type(definition['transforms']) is list:
				for transform in definition['transforms']:
					if transform == 'lower':
						value = value.lower()
					elif transform == 'upper':
						value = value.upper()
					elif transform == 'remove_extra_spaces':
						value = re.sub(r'  +', ' ', value)
			new_record[key] = value
		elif field_type == 'int':
			float_value = convert_to_float(value)
			if float_value is not None and 'multiplier' in definition:
				multiplier = convert_to_float(definition['multiplier'])
				if multiplier is not None:
					value = float_value * multiplier
			value = convert_to_int(round_half_up(value))
			if value is not None:
				new_record[key] = value
		elif field_type == 'float':
			value = convert_to_float(value)
			if value is not None:
				new_record[key] = value
			if 'multiplier' in definition:
				multiplier = convert_to_float(definition['multiplier'])
				if multiplier is not None:
					new_record[key] = new_record[key] * multiplier
		elif field_type == 'bool':
			new_record[key] = False
			if 'true' in definition and type(definition['true']) is list:
				for test in definition['true']:
					test_match = re.compile(r'\b{}\b'.format(test), re.IGNORECASE)
					if re.search(test_match, value):
						new_record[key] = True
			elif 'false' in definition and type(definition['false']) is list:
				new_record[key] = True
				for test in definition['false']:
					test_match = re.compile(r'\b{}\b'.format(test), re.IGNORECASE)
					if re.search(test_match, value):
						new_record[key] = False
			elif value:
				new_record[key] = True
		elif field_type == 'datetime':
			if 'time_field_name' in definition and definition['time_field_name'] in record and record[definition['time_field_name']]:
				time_str = convert_string_to_time(record[definition['time_field_name']])
				value = str(value) + ' ' + str(time_str)
			value = convert_string_to_datetime(value)
			if value is not None:
				new_record[key] = value
		elif field_type == 'date':
			value = convert_string_to_date(value)
			if value is not None:
				new_record[key] = value
		elif field_type == 'time':
			value = convert_string_to_time(value)
			if value is not None:
				new_record[key] = value
	
	_map_csv_last_record = new_record
	return new_record

"""
success = common.write_csv(filepath, array_of_dicts, fields=array_of_fields_to_include):
"""
def write_csv(filepath, data, fields=None):
	filepath = os.path.expanduser(filepath)
	with open(filepath, 'w', newline='') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=fields, extrasaction='ignore')
		writer.writeheader()
		for row in data:
			writer.writerow(row)
	return True


## Environment variables

"""
environment = common.get_environment()
"""
def get_environment():
	if os.environ.get('ENVIRONMENT') in ['prod', 'production', 'main'] or os.environ.get('X-ENVIRONMENT') in ['prod', 'production', 'main']:
		return 'production'
	if re.search(r'-(prod|main)-', os.environ.get('AWS_LAMBDA_FUNCTION_NAME', '')):
		return 'production'
	return 'dev'

"""
env_abbr = common.get_env_abbr()
"""
def get_env_abbr():
	environment = get_environment()
	if environment == 'production':
		return 'prod'
	return 'dev'

"""
dry_run, log_level, limit = common.set_basic_args(event)
"""
def set_basic_args(event):
	dry_run = convert_to_bool(event.get('dry_run')) or False
	log_level = convert_to_int(event.get('log_level')) or 5
	limit = convert_to_int(event.get('limit')) or None
	
	if convert_to_bool(event.get('verbose')):
		log_level = 6
	if convert_to_bool(event.get('extra_verbose')):
		log_level = 7
	
	return dry_run, log_level, limit


## Text handling

"""
normalized_text = common.normalize(text)
"""
def normalize(text, strip_single_chars=True):
	if type(text) is not str:
		return text
	input = text.lower()
	input = unidecode.unidecode(input)
	input = re.sub(r'[^a-z0-9\.\s_-]', '', input)
	input = re.sub(r'[\s\._-]+', ' ', input)
	input = re.sub(r'(?:^ +| +$)', '', input)
	words = input.split(r' ')
	new_words = []
	for word in words:
		if strip_single_chars and len(word) <= 1:
			continue
		new_words.append(word)
	return ' '.join(new_words)

def convert_to_camelcase(text):
# 	text = re.sub(r'[_-](css|html|ics|ip|json|oid|php|rss|smtp|sql|ssl|url|utf|xml|xslt?)(_|$)', r'\1'.upper() + r'\2', text)
	parts = text.split('_')
	text = parts[0]
	if len(parts) > 1:
		for part in parts[1:]:
			text += part.title()
	return text

def convert_to_kebabcase(text):
	text = re.sub(r'[\s\.,_]+', '-', text.lower())
	text = re.sub(r'[^a-z0-9-]', '', text)
	text = re.sub(r'(^-|-$)', '', text)
	return text

def convert_to_snakecase(text):
	text = re.sub(r'[\s\.,-]+', '_', text.lower())
	text = re.sub(r'[^a-z0-9_]', '', text)
	text = re.sub(r'(^_|_$)', '', text)
	return text

def escape_for_bash(text):
	return re.sub(r"'", "'\"'\"'", text)

"""
numeric_phrase = plural(number, singular_word)
test_singles = ['half-brother', 'step-child', 'half-ox', 'titmouse', 'goose', 'stoma', 'referendum', 'aurorae', 'wife', 'leaf', 'wolf', 'plateau', 'thesis', 'vertebra', 'case', 'house', 'key', 'story', 'bus', 'vertex', 'hutch', 'mantis', 'rice', 'mouse']
"""
def plural(number, text):
	text = str(number) + " " + text
	if number == 1:
		return text
	
	if re.search(r'brother$', text):
		return re.sub(r'brother$', 'brethren', text)
	if re.search(r'child$', text):
		return re.sub(r'child$', 'children', text)
	if re.search(r'\bgoose$', text):
		return re.sub(r'\bgoose$', 'geese', text)
	if re.search(r'mouse$', text):
		return re.sub(r'mouse$', 'mice', text)
	if re.search(r'ox$', text):
		return re.sub(r'ox$', 'oxen', text)
	if re.search(r'ma$', text):
		return re.sub(r'ma$', 'mata', text)
	if re.search(r'([ae])ndum$', text):
		return re.sub(r'([ae])ndum$', r'\1nda', text)
	if re.search(r'rae$', text):
		return text
	if re.search(r'([ei])fe$', text):
		return re.sub(r'([ei])fe$', r'\1ves', text)
	if re.search(r'([ei])af$', text):
		return re.sub(r'([ei])af$', r'\1aves', text)
	if re.search(r'f$', text):
		return re.sub(r'f$', 'ves', text)
	if re.search(r'eau$', text):
		return text + 's'
	if re.search(r'esis$', text):
		return re.sub(r'esis$', 'eses', text)
	if re.search(r'([^aeiouy])a$', text):
		return re.sub(r'([^aeiouy])a$', r'\1ae', text)
	if re.search(r'ase$', text):
		return text + 's'
	if re.search(r'ouse$', text):
		return text + 's'
	if re.search(r'ey$', text):
		return text + 's'
	if re.search(r'y$', text):
		return re.sub(r'y$', 'ies', text)
	if re.search(r'us$', text):
		return re.sub(r'us$', 'uses', text)
	if re.search(r'([^aeiouy])ex$', text):
		return re.sub(r'([^aeiouy])ex$', r'\1ices', text)
	if re.match(r'(?:[isux]|ch|sh|[aeiu]o)s$', text):
		return text
	if re.search(r'(s|x|ch|sh)$', text):
		return re.sub(r'(s|x|ch|sh)$', r'\1es', text)
	return text + 's'

def conjunction(orig_words, conj='and'):
	conj = ' ' + conj + ' '
	if type(orig_words) is not list:
		return str(orig_words)
	if len(orig_words) < 1:
		return ''
	
	words = []
	for word in orig_words:
		words.append(str(word))
	if len(words) == 1:
		return words[0]
	elif len(words) == 2:
		return conj.join(words)
	elif len(words) > 2:
		return ', '.join(words[:len(words)-1]) + ',' + conj + words[-1]


## Number handling

def round_half_up(n, decimals=0):
	multiplier = 10**decimals
	return math.floor(n * multiplier + 0.5) / multiplier


## Hash handling

"""
Collapses inner dicts by combining keys with hyphens. A key with the same name as the enclosing dict is named for the enclosing dict.
flat_hash = common.flatten_hash(full_hash)
"""
def flatten_hash(input, upper_key=None, inner_key=None):
	if type(input) is not dict:
		return None
	output = {}
	for key, value in input.items():
		new_key = key
		if inner_key == key:
			new_key = upper_key
		elif upper_key:
			new_key = upper_key + '-' + key
		
		if type(value) is dict:
			sub_hash = flatten_hash(value, new_key, key)
			for subkey, subvalue in sub_hash.items():
				output[subkey] = subvalue
		else:
			output[new_key] = value
	return output


## List handling
"""
Converts what it can into a list. "unique=True" makes output list unique.

list = to_list(str or int or float or bool)
	[str or int or float or bool]
list = to_list(input_list)
	input_list
list = to_list(hash)
	[value1, value2, ...]
list = to_list(list_of_hashes, key)
	[hash1_key_value, hash2_key_value, ...]
list = to_list(hash_of_hashes, key)
	[hash1_key_value, hash2_key_value, ...]
"""
def to_list(input=None, key=None, unique=False):
	output = []
	if key:
		if type(input) is list:
			for item in input:
				if type(item) is dict and key in item:
					output.append(item[key])
		if type(input) is dict:
			if key:
				for k, item in input.items():
					if type(item) is dict and key in item:
						output.append(item[key])
			else:
				output = list(input.values())
	elif type(input) is str or type(input) is int or type(input) is float:
		output = [input]
	elif type(input) is list:
		output = input
	if unique:
		return unique_list(output)
	return output

def unique_list(full_list):
	return list(dict.fromkeys(full_list))

def combine_lists_of_hashes(lists, key_list):
	if len(lists) != len(key_list):
		raise IndexError("Number of lists doesn't match number of keys")
	new_list = []
	main_list = lists.pop(0)
	main_key = key_list.pop(0)
	if not len(lists):
		return main_list
	for element in main_list:
		for i in range(len(lists)):
			sub_list = lists[i]
			sub_key = key_list[i]
			for item in sub_list:
				if element[main_key] == item[sub_key]:
					element.update(item)
		new_list.append(element)
	return new_list


## Variable types and casting

"""
Checks for a datetime datetime, date, or time object.
boolean = common.is_datetime_type(input)
"""
def is_datetime_type(input):
	if is_datetime(input) or is_date(input) or is_time(input):
		return True
	return False

"""
Checks for a datetime datetime object.
boolean = common.is_datetime(input)
"""
def is_datetime(input):
	if type(input) is type(datetime.datetime.utcnow()):
		return True
	return False

"""
Checks for a datetime date object.
boolean = common.is_date(input)
"""
def is_date(input):
	if type(input) is type(datetime.date.today()):
		return True
	return False

"""
Checks for a datetime time object.
boolean = common.is_time(input)
"""
def is_time(input):
	now = datetime.datetime.utcnow()
	if type(input) is type(now.time()):
		return True
	return False

"""
converted_input = common.convert_datetime_to_string(any_input)
"""
def convert_datetime_to_string(input):
	if type(input) is list:
		output = []
		for element in range(len(input)):
			output.append(convert_datetime_to_string(input[element]))
		return output
	elif type(input) is dict:
		output = {}
		for key, value in input.items():
			output[key] = convert_datetime_to_string(value)
		return output
	elif is_datetime(input):
		return input.isoformat(' ')
	elif is_time(input) or is_date(input):
		return input.isoformat()
	return input

"""
datetime_object = common.convert_string_to_datetime(datetime_string)
"""
def convert_string_to_datetime(input):
	if is_datetime(input):
		return input
	input = str(input)
	input = input.strip()
	input = re.sub(r'^(\d{1,4}[-/]\d{1,2}[-/]\d{1,4})[t ](\d{1,2}:\d\d(:\d\d)?( (am|pm))?).*?$', r'\1 \2', input.lower())
	for date_format in ['%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%d/%m/%Y', '%m/%d/%y', '%d/%m/%y']:
		for time_format in ['%I:%M:%S %p', '%H:%M:%S%z']:
			datetime_format = date_format + ' ' + time_format
			try:
				datetime_obj = datetime.datetime.strptime(input + '+0000', datetime_format)
				return datetime_obj
			except ValueError:
				continue
	date_obj = convert_string_to_date(input)
	if date_obj is not None:
		try:
			datetime_obj = datetime.datetime.strptime(date_obj.isoformat() + ' 00:00:00+0000', '%Y-%m-%d %H:%M:%S%z')
			return datetime_obj
		except ValueError:
			pass
	return None

def convert_string_to_date(input):
	if is_datetime(input):
		return input.date()
	if is_date(input):
		return input
	input = str(input)
	input = input.strip()
	input = re.sub(r'^(\d{1,4}[-/]\d{1,2}[-/]\d{1,4}).*$', r'\1', input)
	for date_format in ['%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%d/%m/%Y', '%m/%d/%y', '%d/%m/%y']:
		try:
			datetime_obj = datetime.datetime.strptime(input, date_format)
			return datetime_obj.date()
		except ValueError:
			continue
	return None

def convert_float_to_time(input):
	text = str(input)
	hour = convert_to_int(input)
	minutes = 0
	if re.search(r'\.', text):
		parts = text.split(r'.')
		hour = convert_to_int(parts[0])
		minutes = int(round_half_up(int(parts[1]) * 0.6, 0))
	input = f"{hour:02d}:{minutes:02d}:00"
	try:
		datetime_obj = datetime.datetime.strptime(input, '%H:%M:%S')
		return datetime_obj.time()
	except ValueError:
		return None
	return None

def convert_string_to_time(input):
	if is_datetime(input):
		return input.time()
	if is_time(input):
		return input
	if type(input) is float or re.match(r'\d\d?\.\d+$', input):
		return convert_float_to_time(input)
	input = str(input)
	input = input.strip()
	input = re.sub(r'^(\d{1,2}:\d\d(:\d\d)?( (am|pm))?).*?$', r'\1', input.lower())
	for time_format in ['%I:%M:%S %p', '%H:%M:%S']:
		try:
			datetime_obj = datetime.datetime.strptime(input, time_format)
			return datetime_obj.time()
		except ValueError:
			continue
	return None

"""
boolean = common.is_int(input)
* Accepts strings and floats that can be converted to integers
"""
def is_int(input):
	if type(input) is int:
		return True
	if type(input) is float:
		input = str(input)
	if type(input) is str and re.match(r'-?\d+(\.0*)?', input):
		return True
	return False

"""
integer = common.convert_to_int(input)
* Converts None, booleans, integers, strings, and floats to integers; returns None if not convertible
"""
def convert_to_int(input):
	if input is None:
		return 0
	if type(input) is bool:
		if input:
			return 1
		return 0
	if type(input) is int:
		return input
	if type(input) is float:
		input = str(input)
	if type(input) is str:
		if not input:
			return 0
		if re.match(r'-?\d+(\.0*)?', input):
			return int(re.sub(r'\..*$', '', input))
	return None

"""
boolean = common.is_float(input)
* Accepts strings and integers that can be converted to floats
"""
def is_float(input):
	if type(input) is float:
		return True
	if type(input) is int:
		return True
	if type(input) is str and re.match(r'-?\d+(\.\d*)?', input):
		return True
	return False

"""
float = common.convert_to_float(input)
* Converts None, booleans, integers, strings, and floats to floats; returns None if not convertible
"""
def convert_to_float(input):
	if input is None:
		return 0.0
	if type(input) is bool:
		if input:
			return 1.0
		return 0.0
	if type(input) is int:
		return float(input)
	if type(input) is float:
		return input
	if type(input) is str:
		if not input:
			return 0.0
		if re.match(r'-?\d+(\.\d*)?', input):
			return float(input)
	return None

"""
boolean = common.convert_to_bool(input)
* Converts None, booleans, integers, strings, and floats to integers; returns None if not convertible
* A non-zero integer is true; 0 is false
* A string of 'true', 'yes', or 'on' is true regardless of case; a string of empty, 'false', 'no', or 'off' is false
"""
def convert_to_bool(input):
	if input is None:
		return False
	if type(input) is bool:
		return input
	if is_int(input):
		if convert_to_int(input) == 0:
			return False
		return True
	if type(input) is str:
		input = input.lower()
		if input == 'true' or input == 'yes' or input == 'on':
			return True
		if not input or input == 'false' or input == 'no' or input == 'off':
			return False
	return None

"""
boolean = common.is_uuid(input)
"""
def is_uuid(input):
	if type(input) is str and re.match(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', input):
		return True
	return False


## Date/time formatting

def get_datetime_from_string(input):
	if is_datetime(input):
		return input
	if type(input) is str:
		dt = datetime.datetime.fromisoformat(input)
		if dt:
			return dt
	return None

def get_dt_now(format=None):
	if format == 'date':
		return datetime.date.today()
	now = datetime.datetime.utcnow()
	if format == 'time':
		return now.time()
	return now

def get_dt_past(days=0):
	tz = datetime.timezone(datetime.timedelta(hours=0))
	return datetime.datetime.now(tz) - datetime.timedelta(days=days)

def get_dt_from_epoch(input):
	epoch = convert_to_int(input)
	return datetime.datetime.fromtimestamp(epoch)


"""
datetime_string = common.get_datetime_string(string_or_datetime)
datetime_string = common.get_datetime_string()
"""
def get_datetime_string(input=None):
	if input is None:
		input = get_dt_now()
	elif type(input) is str:
		input = convert_string_to_datetime(input)
	if not is_datetime(input):
		return input
	return convert_datetime_to_string(input)

"""
date_string = common.get_date_string(string_or_datetime)
date_string = common.get_date_string()
"""
def get_date_string(input=None):
	if input is None:
		input = get_dt_now()
	elif type(input) is str:
		input = datetime.datetime.fromisoformat(input)
	if not is_datetime(input):
		return input
	month = input.strftime("%b")
	day = str(input.day)
	year = input.strftime("%Y")
	return f"{month} {day}, {year}"



def get_epoch(dt=None):
	if dt:
		dt = get_datetime_from_string(dt)
	else:	
		dt = get_dt_now()
	return int(dt.timestamp())

def get_epoch_ms():
	dt = get_dt_now()
	return int(dt.timestamp() * 1000)


## Input checking

"""
output, errors = common.check_input([
	[ "key_name", "str", True, 1, 16],
	[ "key_name2", "int", False, 1],
	[ "key_name3", "dict", [
		[ "key_name4", "str", True, 1, 16],
		[ "key_name5", "int", False, 1]
	]]
], input_structure)
* [ key_name, field_type, is_required, min_length, max_length ]
"""
def check_input(field_map, body):
	output = {}
	errors = []
	for field in field_map:
		key = field[0]
		ftype = field[1]
		required = False
		sub_map = None
		if len(field) >= 3:
			if type(field[2]) is bool:
				required = field[2]
			elif type(field[2]) is list:
				sub_map = field[2]
		min_length = 0
		if len(field) >= 4 and type(field[3]) is int:
			min_length = field[3]
		max_length = 0
		if len(field) >= 5 and type(field[4]) is int:
			max_length = field[4]
		elif ftype == 'int':
			max_length = 2147483647
		if ftype == 'bool':
			ftype = 'boolean'
		
		if key not in body:
			if (type(required) is list or required):
				errors.append("'{}' is required".format(key))
			continue
		if type(required) is list:
			if body[key] not in required:
				errors.append("'{}' must be one of '{}'".format(key, "', '".join(required)))
		
		if ftype == "str":
			if type(body[key]) is not str:
				errors.append("'{}' must be a string".format(key))
				continue
		elif ftype == "int":
			if not is_int(body[key]):
				errors.append("'{}' must be an integer".format(key))
				continue
		elif ftype == "boolean":
			if convert_to_bool(body[key]) is None:
				errors.append("'{}' must be a boolean".format(key))
				continue
		elif ftype == "dict":
			if type(body[key]) is not dict:
				errors.append("'{}' must be a structure/hash".format(key))
				continue
		elif ftype == "list":
			if type(body[key]) is not list:
				errors.append("'{}' must be a list/array".format(key))
				continue
		
		if sub_map and ftype in ['str', 'int']:
			if body[key] not in sub_map:
				errors.append("'{}' should be one of '{}'.".format(key, "', '".join(sub_map)))
				continue
			output[key] = str(body[key])
		elif ftype == "str":
			if min_length and len(body[key]) < min_length:
				errors.append("'{}' should be at least {} characters long".format(key, min_length))
				continue
			if max_length and len(body[key]) > max_length:
				errors.append("'{}' should be less than {} characters long".format(key, max_length))
				continue
			output[key] = str(body[key])
		elif ftype == "int":
			if min_length and int(body[key]) < min_length:
				errors.append("'{}' should be greater than {}".format(key, min_length))
				continue
			if max_length and int(body[key]) > max_length:
				errors.append("'{}' should be less than {}".format(key, max_length))
				continue
			output[key] = convert_to_int(body[key])
		elif ftype == "boolean":
			output[key] = convert_to_bool(body[key])
		elif ftype == "dict" or ftype == "list":
			if len(field) >= 4:
				sub_output, sub_errors = check_input(field[3], body[key])
				if len(sub_output):
					output[key] = sub_output
				if len(sub_errors):
					errors.extend(sub_errors)
			else:
				output[key] = body[key]
	return output, errors
		

## Logging

def normalize_log_level(value):
	if is_int(value) and value >= 0 and value <= 7:
		return convert_to_int(value)
	elif type(value) is str:
		if re.match(r'emerg'):
			return 0
		elif re.match(r'alert'):
			return 1
		elif re.match(r'crit'):
			return 2
		elif re.match(r'err'):
			return 3
		elif re.match(r'warning'):
			return 4
		elif re.match(r'notice'):
			return 5
		elif re.match(r'info'):
			return 6
		elif re.match(r'debug'):
			return 7
	return 0


# Serverless functions

def is_local():
	if 'LAMBDA_TASK_ROOT' not in os.environ or ('IS_LOCAL' in os.environ and os.environ['IS_LOCAL'] == 'true'):
		if 'USER' in os.environ and os.environ['USER'] != 'ubuntu':
			return True
	return False


# AWS functions

"""
!!! DEPRECATED - Use convert_dict_to_list() instead, but the default case switches.

tags_list = common.convert_tags(tags_dict, case)
tags_list = common.convert_tags(
	{
		'key': "value",
		'key2': "value2"
	},
	'lower'|'upper' # defaults to 'lower'
)

'lower' uses lowercase labels ('key', 'value') required by ECS and others
'upper' uses uppercase labels ('Key', 'Value') required by ELBv2 and others
"""
def convert_tags(tags_dict, case='lower'):
	if type(tags_dict) is list:
		return tags_dict
	if type(tags_dict) is not dict:
		return []
	
	tags_list = []
	for key, value in tags_dict.items():
		if case == 'upper':
			tags_list.append({
				'Key': key,
				'Value': value
			})
		else:
			tags_list.append({
				'key': key,
				'value': value
			})
		
	return tags_list

"""
output_list = common.convert_dict_to_list(input_dict)

'upper' uses uppercase labels ('Key', 'Value') required by ELBv2 and others
'lower' uses lowercase labels ('key', 'value') required by ECS and others
"""
def convert_dict_to_list(input_dict, case='upper'):
	if type(input_dict) is list:
		return input_dict
	if type(input_dict) is not dict:
		return []
	
	output_list = []
	for key, value in input_dict.items():
		if case == 'upper':
			output_list.append({
				'Key': key,
				'Value': value
			})
		else:
			output_list.append({
				'key': key,
				'value': value
			})
		
	return output_list

"""
output_dict = common.convert_list_to_dict(input_list)
"""
def convert_list_to_dict(input_list):
	if type(input_list) is dict:
		return input_list
	if type(input_list) is not list:
		return {}
	
	output_dict = {}
	for element in input_list:
		if type(element) is not dict:
			continue
		key = None
		if 'Key' in element:
			key = element['Key']
		elif 'key' in element:
			key = element['key']
		else:
			continue
		value = None
		if 'Value' in element:
			value = element['Value']
		elif 'value' in element:
			value = element['value']
		else:
			continue
		
		output_dict[key] = value
		
	return output_dict	

"""
unquoted_list = common.unquote_list(quoted_list)
"""
def unquote_list(quoted_list):
	for item in quoted_list:
		unquoted = item
		if re.match(r"^'", item):
			unquoted = re.sub(r"(^'|'$)", '', item)
		elif re.match(r'^"', item):
			unquoted = re.sub(r'(^"|"$)', '', item)
	return unquoted

"""
args = common.cast_args(args, specs)
"""
def cast_args(input={}, specs={}, should_fill=False):
	if type(input) is not dict:
		return input
	if type(specs) is not list and type(specs) is not dict:
		return input
	
	if type(specs) is list:
		for key in specs:
			if key not in input and should_fill:
				args[key] = None
			else:
				args[key] = input[key]
		return args
	
	if type(specs) is dict:
		for key, value in specs.items():
			if key not in input and should_fill:
				args[key] = None
				continue
			# Boolean
			elif (value == 'boolean' or value == 'True|False') and input[key]:
				args[key] = True
			elif (value == 'boolean' or value == 'True|False'):
				args[key] = False
			# Integer
			elif value == 'integer' or type(value) is int:
				args[key] = int(input[key])
			# String
			elif value == 'string' and re.search(r'\|', value):
				args[key] = str(input[key])
				options = unquote_list(value.split('|'))
				if args[key] not in options:
					raise AttributeError("Arg '{}' is '{}', but must be one of {}.".format(key, input[key], value))
			elif value == 'string':
				args[key] = str(input[key])
			elif value == 'list' or type(value) is list:
				if type(input[key]) is not list:
					raise AttributeError("Arg '{}' is type '{}' but must be type list.".format(key, type(input[key])))
				args[key] = input[key]
			elif value == 'dict' or type(value) is dict:
				if type(input[key]) is not dict:
					raise AttributeError("Arg '{}' is type '{}' but must be type dict.".format(key, type(input[key])))
				args[key] = input[key]
	return args

"""
boolean = common.is_success(response)
"""
def is_success(response):
	if type(response) is dict and 'ResponseMetadata' in response:
		if response['ResponseMetadata'].get('HTTPStatusCode') == 200 or response['ResponseMetadata'].get('HTTPStatusCode') == 204:
			return True
	return False

