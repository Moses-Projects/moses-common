# print("Loaded common init")

import base64
from charset_normalizer import from_bytes
import collections.abc
import csv
import datetime
import decimal
from email.utils import parseaddr
import gzip
import io
import json
import math
import os
import re
import requests
import secrets
import sys
import unidecode
import urllib.request
import urllib.parse
import uuid
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
def get_url(url, args={}, convert=True, debug=False, dry_run=False, log_level=5):
	if debug:
		log_level = 7

	if type(args) is not dict:
		raise AttributeError("get_url arg should be a dict.")

	headers = {}
	if 'headers' in args and type(args['headers']) is dict:
		headers = args['headers']

	if args.get('bearer_token'):
		headers['Authorization'] = 'Bearer {}'.format(args['bearer_token'])
	elif args.get('username') and args.get('password'):
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
	if args.get('method'):
		method = args['method']
	elif data:
		method = 'POST'

	if 'query' in args and type(args['query']) is dict:
		query = urllib.parse.urlencode(args['query'])
		url += '?' + query

	if log_level >= 7 or dry_run:
		print("method:", method)
		print("url:", url)
		print("headers: {}".format(headers))
		if data:
			print("data {}: {}".format(type(data), data))
	elif log_level >= 6:
		print(f"{method} {url}")

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

	if convert:
		return 200, convert_value(response)
	else:
		return 200, response

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

def url_decode(query_string):
	return urllib.parse.parse_qs(query_string)


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
	base64_string = base64_string.strip()
	if not re.match(r'[A-Za-z0-9\+/]+={0,2}$', base64_string):
		return None
	try:
		base64_object = base64.b64decode(base64_string).decode('utf-8')
		return base64_object
	except ValueError as e:
		return base64_string
	return base64_string

"""
base64_string = common.make_base64(string)
"""
def make_base64(text):
	b64_bytes = base64.b64encode(text.encode('utf-8'))
	return b64_bytes.decode('utf-8')


"""
csv_text = common.make_csv(python_object)
"""
def make_csv(python_object, fields=None):
	new_csvfile = io.StringIO()
	if not fields:
		fields = python_object[0].keys()

	csv_writer = csv.DictWriter(new_csvfile, fieldnames=fields)
	csv_writer.writeheader()
	csv_writer.writerows(python_object)
	output = new_csvfile.getvalue()
	new_csvfile.close()

	return output


"""
boolean = common.is_gzip(gzip_bytes)
"""
def is_gzip(gzip_bytes):
	if type(gzip_bytes) is not bytes:
		return False
	if gzip_bytes[0:2] == b'\x1f\x8b':
		return True
	return False

"""
output = common.decompress_gzip(gzip_bytes)
"""
def decompress_gzip(gzip_bytes):
	if not is_gzip(gzip_bytes):
		return None
	try:
		output = gzip.decompress(gzip_bytes)
	except ValueError as e:
		print(e)
		return None
	return output


def _strip_backslash_before_single_quote(s):
    # For any run of backslashes immediately before a single quote,
    # if the count is odd, drop exactly one (so \\' -> \\', \' -> ').
    def repl(m: re.Match) -> str:
        n = len(m.group(0))  # number of backslashes
        if n % 2 == 1:
            return '\\' * (n - 1)  # remove exactly one
        return m.group(0)          # even count: leave as-is
    # Match one or more backslashes only when directly before a single quote
    return re.sub(r'\\+(?=\')', repl, s)

def _escape_other_invalid_backslashes(s):
    # Double any backslash that does not start a valid JSON escape
    # (so \x becomes \\x), but keep valid ones (\n, \t, \uXXXX, \", \\, \/)
    return re.sub(r'\\(?!["\\/bfnrtu])', r'\\\\', s)

"""
boolean = common.is_json(json_string)
"""
def is_json(json_string):
	if type(json_string) is not str:
		return False
	json_object = parse_json(json_string)
	if json_object is None:
		return False
	return True

"""
json_object = common.parse_json(json_string)
"""
def parse_json(json_string):
	if type(json_string) is dict or type(json_string) is list:
		return json_string
	if type(json_string) is not str:
		return None
	json_string = json_string.strip()
	if not re.match(r'(\{|\[)', json_string):
		return None
	try:
		# Remove invalid single quote escapes
# 		json_string = re.sub(r'(?<!\\)\\([^u"])', r'\1', json_string)
		json_string = _strip_backslash_before_single_quote(json_string)
		json_string = _escape_other_invalid_backslashes(json_string)
		json_object = json.loads(json_string)
	except ValueError as e:
		print(e)
		return None
	return json_object

"""
json_string = common.make_json(python_object)
json_string = common.make_json(python_object, pretty_print=True, sort_keys=True)
"""
def make_json(python_object, pretty_print=False, sort_keys=False):
	if is_json(python_object):
		python_object = parse_json(python_object)
	python_object = convert_datetime_to_string(python_object)
	if pretty_print:
		return json.dumps(python_object, sort_keys=sort_keys, indent=2)
	return json.dumps(python_object, sort_keys=sort_keys)


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
	xml_string = xml_string.strip()
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
	yaml_string = yaml_string.strip()
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
	return '---\n' + yaml.dump(python_object)


"""
value_object = common.convert_value(value_string)
  Recognizes and converts Base64, JSON, XML, and YAML into a Python object.
"""
def convert_value(input_string):
	if is_gzip(input_string):
		input_string = decompress_gzip(input_string)
	if type(input_string) is bytes:
		input_string = str(from_bytes(input_string).best())

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

def get_filename_timestamp():
	tz = datetime.timezone(datetime.timedelta(hours=0))
	now = datetime.datetime.now(tz)
	return now.strftime("%Y_%m_%d-%H_%M_%S")

def get_filepath(name=None, format=None):
	path = ''
	if os.environ.get('HOME'):
		if os.path.isdir(path + 'Downloads'):
			path = os.path.expanduser(os.environ.get('HOME')) + '/Downloads/'
	if name:
		path += convert_to_snakecase(normalize(name)) + '-'
	path += get_filename_timestamp()
	if format:
		path += '.' + format
	return path

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
	if re.search(r'\.csv$', filepath, re.IGNORECASE) and convert:
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
success = common.write_file(filename, data, format='json', make_dir=False)
"""
def write_file(filepath, data, format=None, make_dir=False):
	text = str(data)
	if type(data) is list or type(data) is dict:
		if format == 'json' or re.search(r'\.json$', filepath):
			text = make_json(data)
		elif format == 'xml' or re.search(r'\.xml$', filepath):
			text = make_xml(data)
		elif format == 'yaml' or re.search(r'\.ya?ml$', filepath):
			text = make_yaml(data)
		else:
			return False
	filepath = os.path.expanduser(filepath)
	if make_dir:
		base_dir = os.path.dirname(filepath)
		os.makedirs(base_dir, exist_ok=True)

	with open(filepath, "w") as file:
		file.write(text)
		return True

def get_script_name():
	script_name = os.path.basename(sys.modules['__main__'].__file__)
	if re.search(r'\.\w+$', script_name):
		script_name = re.sub(r'\.\w+$', '', script_name)
	return script_name

def get_storage_dir(name=None):
	base_dir = os.environ.get('HOME', '')
	if not name:
		name = get_script_name()
	script_path = f"{base_dir}/.moses_common/{name}"
	return script_path

"""
data = common.read_cache(filename, days_to_expire)
"""
def read_cache(filepath, days):
	filepath = os.path.expanduser(filepath)
	if not os.path.isfile(filepath):
		return None
	age = get_file_age(filepath, file_checked=True)
# 	print("age {}: {}".format(type(age), age))
	if convert_to_float(days) < get_file_age(filepath, file_checked=True):
		return None

	return read_file(filepath, file_checked=True)

"""
settings = common.read_config()
settings = common.read_config(filename)
"""
def read_config(filename=None):
	if not filename:
		filename = get_storage_dir() + f"/settings.yml"
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
		os.makedirs(get_storage_dir(), exist_ok=True)
		filename = get_storage_dir() + f"/settings.yml"
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
records = common.read_csv(filepath, delimiter=',')
records = common.read_csv(filepath, delimiter=',', add_row_number_field="item_num", mapping=csv_mapping)
"""
def read_csv(filepath, delimiter=',', mapping=None, add_row_number_field=None):
	filepath = os.path.expanduser(filepath)
	if not os.path.isfile(filepath):
		return None
	records = []
	with open(filepath) as csv_file:
		csv_read = csv.reader(csv_file, delimiter=delimiter)

		headers = []
		if add_row_number_field:
			headers.append(add_row_number_field)
		cnt = 0
		for item in csv_read:
			if cnt == 0:
				for field in item:
					headers.append(convert_to_snakecase(field))
			else:
				record = {}
				if add_row_number_field:
					item.insert(0, cnt)
				for i in range(len(headers)):
					if len(item) >= i:
						record[headers[i]] = item[i]
				records.append(record)
			cnt += 1
	if mapping and type(mapping) is dict:
		return map_csv(records, mapping)
	return records

"""
new_records = common.map_csv(records, {
	"field": "csv_field_name",
	"field": { "name": "csv_field_name" },
	"field": { "name": "csv_field_name", "type": "str", "transforms": ['lower', 'remove_extra_spaces', 'pad0=3'] },
	"field": { "name": "csv_field_name", "type": "int", "multiplier": 100 },
	"field": { "name": "csv_field_name", "type": "float" },
	"field": { "name": "csv_field_name", "type": "bool", "true": ['yes', 'y'] },
	"field": { "name": "csv_field_name", "type": "map", "map": { "value_from_import_field": "value_to_use" } },
	"field": { "type": "literal", "value": "value_of_any_type" }
})
type: 'str', 'int', 'float', 'bool', 'datetime', 'date', 'time', 'map'
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
		if 'type' in definition and definition['type'] in ['str', 'int', 'float', 'bool', 'datetime', 'date', 'time', 'map']:
			field_type = definition['type']

		if not value:
			if 'carry' in definition and definition['carry'] and key in _map_csv_last_record:
				new_record[key] = _map_csv_last_record[key]
		elif field_type == 'str' or field_type == 'map':
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
					elif re.match(r'left=', transform, re.I):
						value = value[0:int(transform[5:])]
					elif re.match(r'pad0=', transform, re.I):
						value = value.zfill(int(transform[5:]))
			if field_type == 'map':
				if definition.get('map') and type(definition['map']) is dict and value in definition['map']:
					new_record[key] = definition['map'][value]
			else:
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
success = common.write_csv(filepath, array_of_dicts, fields=array_of_fields_to_include)
"""
def write_csv(filepath, data, fields=None, include_header=True, make_dir=False):
	filepath = os.path.expanduser(filepath)
	if not data:
		include_header = False
	elif not fields and type(data[0]) is dict:
		fields = data[0].keys()
	if make_dir:
		base_dir = os.path.dirname(filepath)
		os.makedirs(base_dir, exist_ok=True)
	with open(filepath, 'w', newline='') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=fields, extrasaction='ignore')
		if include_header:
			writer.writeheader()
		for row in data:
			writer.writerow(row)
	return True


## Environment variables

"""
environment = common.get_environment()
"""
def get_environment(env_input=None):
	if env_input:
		if env_input in ['prod', 'production', 'main']:
			return 'production'
	elif os.environ.get('ENVIRONMENT') in ['prod', 'production', 'main'] or os.environ.get('X-ENVIRONMENT') in ['prod', 'production', 'main']:
		return 'production'
	return 'dev'

"""
function_stage = common.get_function_stage()
"""
def get_function_stage():
	if os.environ.get('STAGE') in ['main', 'prod', 'production']:
		return 'main'
	elif re.search(r'-(prod|main)-', os.environ.get('AWS_LAMBDA_FUNCTION_NAME', '')):
		return 'main'
	return 'dev'

"""
env_abbr = common.get_env_abbr()
"""
def get_env_abbr(env_input=None):
	environment = get_environment(env_input)
	if environment == 'production':
		return 'prod'
	return 'dev'

"""
dry_run, log_level, limit = common.set_basic_args(event)
"""
def set_basic_args(event):
	dry_run = False
	# From direct CLI
	if 'dry_run' in event:
		dry_run = convert_to_bool(event['dry_run']) or False
# 		if dry_run:
# 			print("dry_run from event")
# 			print("event {}: {}".format(type(event), event))
	# From CLI invoke
	elif 'OPT_DRY_RUN' in os.environ:
		dry_run = convert_to_bool(os.environ['OPT_DRY_RUN']) or False
# 		if dry_run:
# 			print("dry_run from env var")
# 			print("os.environ {}: {}".format(type(os.environ), os.environ))
	# From API or AWS invoke
	elif 'headers' in event:
		for header, value in event['headers'].items():
			if header.upper() == 'X-DRY-RUN' and convert_to_bool(value):
				dry_run = True
# 				print("dry_run from header")
# 				print("event['headers'] {}: {}".format(type(event['headers']), event['headers']))
				break

	log_level = 5
	# From direct CLI
	if convert_to_bool(event.get('extra_verbose')):
		log_level = 7
	elif convert_to_bool(event.get('verbose')):
		log_level = 6
	elif 'log_level' in event:
		log_level = convert_to_int(event['log_level']) or 5
	# From CLI invoke
	elif 'OPT_EXTRA_VERBOSE' in os.environ:
		log_level = 7
	elif 'OPT_VERBOSE' in os.environ:
		log_level = 6
	elif 'OPT_LOG_LEVEL' in os.environ:
		log_level = convert_to_int(os.environ['OPT_LOG_LEVEL']) or 5
	# From API or AWS invoke
	elif 'headers' in event:
		for header, value in event['headers'].items():
			if header.upper() == 'X-VERBOSE' and convert_to_bool(value):
				log_level = 6
				break
			elif header.upper() == 'X-EXTRA-VERBOSE' and convert_to_bool(value):
				log_level = 7
				break
			elif header.upper() == 'X-LOG-LEVEL':
				log_level = convert_to_int(value) or 5
				break

	limit = None
	# From direct CLI
	if 'limit' in event:
		event['limit'] = convert_to_int(event['limit']) or None
		limit = event['limit']
	# From CLI invoke
	elif 'OPT_LIMIT' in os.environ:
		event['limit'] = convert_to_int(os.environ['OPT_LIMIT']) or None
		limit = event['limit']
	elif 'headers' in event:
		for header, value in event['headers'].items():
			if header.upper() == 'X-LIMIT':
				event['limit'] = convert_to_int(value) or None
				limit = event['limit']
				break

	if log_level >= 7:
		print(f"event: {event}")
	if log_level >= 6:
		if dry_run:
			print("Dry run, ", end='')
		print(f"Log level ({log_level})", end='')
		if limit:
			print(f", Limit ({limit})", end='')
		print('')

	return dry_run, log_level, limit


## UUID handling
"""
uuid = common.generate_uuid()
"""
def generate_uuid():
	return str(uuid.uuid4())

"""
doc_id = common.generate_doc_id()
"""
def generate_doc_id(length=22):
	alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
	return ''.join(secrets.choice(alphabet) for _ in range(length))


## Text handling

"""
normalized_text = common.normalize(text)
normalized_text = common.normalize(text, strip_single_chars=True)
"""
def normalize(text, strip_single_chars=True, delimiter=' '):
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
	return delimiter.join(new_words)

def convert_to_ascii(unicode):
	return unidecode.unidecode(unicode)

def convert_to_camelcase(text):
# 	text = re.sub(r'[_-](css|html|ics|ip|json|oid|php|rss|smtp|sql|ssl|url|utf|xml|xslt?)(_|$)', r'\1'.upper() + r'\2', text)
	text = convert_to_str(text)
	parts = text.split('_')
	text = parts[0]
	if len(parts) > 1:
		for part in parts[1:]:
			text += part.title()
	return text

def convert_to_kebabcase(text):
	text = convert_to_str(text)
	text = re.sub(r'[\s\.,_]+', '-', text.lower())
	text = re.sub(r'[^a-z0-9-]', '', text)
	text = re.sub(r'(^-|-$)', '', text)
	return text

def convert_to_snakecase(text):
	text = convert_to_str(text)
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

def conjunction(orig_words, conj='and', quote=''):
	if type(orig_words) is not list:
		return str(quote + orig_words + quote)
	if len(orig_words) < 1:
		return ''

	words = []
	for word in orig_words:
		if type(word) is list:
			subconj = 'or'
			if conj == 'or':
				subconj = 'and'
			words.append('(' + conjunction(word, conj=subconj, quote=quote) + ')')
		else:
			words.append(quote + str(word) + quote)

	conj = ' ' + conj + ' '
	if len(words) == 1:
		return words[0]
	elif len(words) == 2:
		return conj.join(words)
	elif len(words) > 2:
		return ', '.join(words[:len(words)-1]) + ',' + conj + words[-1]

def parse_quoted_args(input_string):
	dquote_match = re.compile(r'^"(.*?)"\s*')
	squote_match = re.compile(r"^'(.*?)'\s*")
	word_match = re.compile(r'^(\S+)\s*')
	args = []
	while input_string:
		dquote_parts = re.match(dquote_match, input_string)
		if dquote_parts:
			args.append(dquote_parts.group(1))
			input_string = re.sub(dquote_match, '', input_string, 1)
			continue
		squote_parts = re.match(squote_match, input_string)
		if squote_parts:
			args.append(squote_parts.group(1))
			input_string = re.sub(squote_match, '', input_string, 1)
			continue
		word_parts = re.match(word_match, input_string)
		if word_parts:
			args.append(word_parts.group(1))
			input_string = re.sub(word_match, '', input_string, 1)
			continue
		if not re.match(r'\S', input_string):
			break
	return args

def get_first_matching_words(titles):
	titles = titles.copy()
	first = titles.pop(0)
	first_words = re.split(r' +', first)
	title_min = 1024
	for title in titles:
		word_min = 0
		title_words = re.split(r' +', title)
		for index in range(len(first_words)):
			if index >= len(title_words) or first_words[index].lower() != title_words[index].lower():
				break
			word_min += 1
		if word_min < title_min:
			title_min = word_min
	matching_words = ' '.join(first_words[0:title_min])
	return matching_words

def add_ordinal_suffix(number):
	number = str(number)
	if re.search(r'1[1-3]$', number):
		return number + 'th'
	elif re.search(r'1$', number):
		return number + 'st'
	elif re.search(r'2$', number):
		return number + 'nd'
	elif re.search(r'3$', number):
		return number + 'rd'
	return number + 'th'

def match_capitalization(original, word):
	if re.match(r'^[^a-z]+$', original):
		return word.upper()
	elif re.match(r'^[A-Z]', original):
		return word.capitalize()
	else:
		return word.lower()

def title_capitalize(text, exceptions=None):
	new_text = text.title()
	# Lowercase word exceptions
	new_text = re.sub(r'\b(a|an|and|as|at|but|by|for|in|nor|of|off|on|or|out|per|so|the|to|up|via|yet)\b', lambda m: f"{m.group(1).lower()}", new_text, 0, re.IGNORECASE)
	# Capitalize acronyms
	new_text = re.sub(r'\b([bcdfghjklmnpqrstvwxyz]+)\b', lambda m: f"{m.group(1).upper()}", new_text, 0, re.IGNORECASE)
	# Capitalize roman numerals
	new_text = re.sub(r'\b(M{0,3}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))\b', lambda m: f"{m.group(1).upper()}", new_text, 0, re.IGNORECASE)
	# Lowercase contractions
	new_text = re.sub(r"(?<=\w)'(d|m|s|t|ll|re|ve)\b", lambda m: f"'{m.group(1).lower()}", new_text, 0, re.IGNORECASE)
	# Capitalize first word
	new_text = new_text[0].upper() + new_text[1:]
	# Handle exceptions
	if exceptions and type(exceptions) is list:
		for exception in exceptions:
			new_text = re.sub(r'\b({})\b'.format(exception), exception, new_text, 0, re.IGNORECASE)
	return new_text

"""
summary = common.truncate(text, limit, type='word')
summary = common.truncate(text, limit, type='character|word|sentence|paragraph')
summary = common.truncate(text, limit, type='word', include_ellipsis=True, remove_newlines=False)
"""
def truncate(text, limit, type='word', include_ellipsis=True, remove_newlines=False):
	output = ''
	cnt = 0

	if remove_newlines:
		text = re.sub(r'\s*\n+\s*', ' ', text, 0)
	text = re.sub(r'\s+', ' ', text.rstrip(), 0)

	if type == 'character':
		if len(text) <= limit:
			return text
		if include_ellipsis:
			output = text[0:limit-1]
			if len(output) != len(text):
				output += '…';
		else:
			output = text[0:limit]
		return output

	while text:
		if cnt >= limit:
			break
		cnt += 1

		part = ''
		if type == 'paragraph':
			pmatch = re.compile(r'^(.+?\n+)')
			parts = re.match(pmatch, text)
			if parts:
				part = parts.group(1)
				text = re.sub(pmatch, '', text, 1)
		elif type == 'sentence':
			pmatch = re.compile(r'^(.+?[\.!\?]+(?:\s+|&\w+;))')
			parts = re.match(pmatch, text)
			if parts:
				part = parts.group(1)
				text = re.sub(pmatch, '', text, 1)
		elif type == 'word':
			pmatch = re.compile(r'^(.+?\s+)')
			parts = re.match(pmatch, text)
			if parts:
				part = parts.group(1)
				text = re.sub(pmatch, '', text, 1)
		if not part:
			output += text
			text = ""
			break
		output += part

	output = output.rstrip()
	if include_ellipsis and text:
		output += '…'
	return output


## Number handling

def round_half_up(n, decimals=0):
	multiplier = 10**decimals
	return math.floor(convert_to_float(n) * multiplier + 0.5) / multiplier


## Hash handling

"""
Collapses inner dicts by combining keys with a delimiter. A key with the same name as the enclosing dict is named for the enclosing dict.
flat_hash = common.flatten_hash(full_hash)
flat_hash = common.flatten_hash(full_hash, delimiter='-', exempt=['field1', 'field2'])
"""
def flatten_hash(input_dict, upper_key=None, inner_key=None, delimiter='-', exempt=[]):
	if type(input_dict) is not dict:
		return None
	output = {}
	for key, value in input_dict.items():
		new_key = key
		if inner_key == key:
			new_key = upper_key
		elif upper_key:
			new_key = upper_key + delimiter + key

		if type(value) is dict and key not in exempt:
			sub_hash = flatten_hash(value, new_key, key, delimiter=delimiter)
			for subkey, subvalue in sub_hash.items():
				output[subkey] = subvalue
		else:
			output[new_key] = value
	return output

"""
Expands keys with delimiter into inner dicts. A key with the same name as the enclosing dict is named for the enclosing dict.
full_hash = common.expand_hash(flat_hash)
full_hash = common.expand_hash(flat_hash, delimiter='-')
"""
def expand_hash(input_dict, upper_key=None, inner_key=None, delimiter='-', exempt=[]):
	if type(input_dict) is not dict:
		return None
	output = {}
	for full_key, value in input_dict.items():
		parts = full_key.split(delimiter)
		current = output

		for i, part in enumerate(parts):
			if i == len(parts) - 1:
				# If we're at the end of the key path, set the value
				if part in current and isinstance(current[part], dict):
					# Conflict: existing dict, keep dict structure
					current[part][full_key] = value
				else:
					current[part] = value
			else:
				# Intermediate keys: create nested dict if needed
				if part not in current or not isinstance(current[part], dict):
					current[part] = {}
				current = current[part]

	return output

"""
Shallow merge of a hash.
common.merge_hash(target_hash, source_hash)
"""
def merge_hash(target, source):
	for key, value in source.items():
		target[key] = value
	return target

"""
Does a deep merge of a hash.
common.update_hash(target_hash, source_hash)
target_hash = common.update_hash({}, source_hash)
"""
def update_hash(target, source):
	if isinstance(source, collections.abc.Mapping):
		for key, value in source.items():
			if isinstance(value, collections.abc.Mapping) or type(value) is list:
				target[key] = update_hash(target.get(key, {}), value)
			else:
				target[key] = value
	elif type(source) is list:
		for value in source:
			if isinstance(value, collections.abc.Mapping) or type(value) is list:
				if type(target) is list:
					target.extend(update_hash(target, value))
				else:
					target = update_hash(target, value)
			else:
				if type(target) is not list:
					target = []
				target.append(value)
	return target

def are_alike(object_a, object_b):
	if type(object_a) is not type(object_a):
		return False
	if type(object_a) is dict:
		for key, value in object_a.items():
			if key not in object_b:
				return False
			elif type(value) is dict or type(value) is list:
				if not are_alike(value, object_b[key]):
					return False
			elif value != object_b[key]:
				return False
		for key, value in object_b.items():
			if key not in object_a:
				return False
			elif type(value) is dict or type(value) is list:
				if not are_alike(value, object_a[key]):
					return False
			elif value != object_a[key]:
				return False
	elif type(object_a) is list:
		if len(object_a) != len(object_b):
			return False
		for i in range(len(object_a)):
			if type(object_a[i]) is dict or type(object_a[i]) is list:
				if not are_alike(object_a[i], object_b[i]):
					return False
			elif object_a[i] != object_b[i]:
				return False
	return True


## List handling
"""
Converts what it can into a list. "unique=True" makes output list unique.

## Without "key" - puts what you give it in a list
# Single value
list = to_list(str or int or float or bool)
	[str or int or float or bool]

# List returns the same list
list = to_list(input_list)
	input_list

# Hash returns a list with the hash as the only element
list = to_list(hash)
	[hash]

## With "key" - pulls values from what you give it to put in a list
# Hash returns value of "key" in hash
list = to_list(hash)
	[value]

# Hash of hashes returns value of "key" in hash values that are hashes
list = to_list(hash_of_hashes, key)
	[hash1_key_value, hash2_key_value, ...]

# List of hashes returns value of "key" from hashes
list = to_list(list_of_hashes, key)
	[hash1_key_value, hash2_key_value, ...]
"""
def to_list(input=None, key=None, unique=False, strip_whitespace=False):
	output = []
	# hashes inside a list or hash
	if key:
		# list of hashes containing key
		if type(input) is list:
			for item in input:
				if type(item) is dict and key in item:
					value = item[key]
					if strip_whitespace and type(value) is str:
						value = item[key].strip()
					output.append(value)
		# hash
		elif type(input) is dict:
			# hash with key
			if key in input:
				value = item[key]
				if strip_whitespace and type(value) is str:
					value = item[key].strip()
				output.append(value)
			# hash with hashes containing key
			else:
				for k, item in input.items():
					if type(item) is dict and key in item:
						value = item[key]
						if strip_whitespace and type(value) is str:
							value = item[key].strip()
						output.append(value)
	# hash without key
	elif type(input) is dict:
		output.append(input)
	# single value
	elif type(input) is str or type(input) is int or type(input) is float or type(input) is bool:
		if strip_whitespace and type(input) is str:
			input = input.strip()
		output = [input]
	# already a list
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
					update_hash(element, item)
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
	tz = datetime.timezone(datetime.timedelta(hours=0))
	if type(input) is type(datetime.datetime.now(tz)):
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
	tz = datetime.timezone(datetime.timedelta(hours=0))
	now = datetime.datetime.now(tz)
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
def convert_string_to_datetime(input, tz_aware=False):
	if is_datetime(input):
		return input
	input = str(input)
	input = input.strip()
# 	input = re.sub(r'^(\d{1,4}[-/]\d{1,2}[-/]\d{1,4})[t ](\d{1,2}:\d\d(:\d\d(.\d+)?([-+]\d\d(:?\d\d)?)?)?( (am|pm))?).*?$', r'\1 \2', input.lower())
	input = re.sub(r'^(\d{1,4}[-/]\d{1,2}[-/]\d{1,4})[t ](\d{1,2}:\d\d(:\d\d(\.\d+)?([-+]\d\d(:?\d\d)?)?)?( (am|pm))?).*?$', r'\1 \2', input.lower())
	if re.match(r'[-+]\d\d(:?\d\d)?', input):
		input += '+0000'
	tz = datetime.timezone(datetime.timedelta(hours=0))
	for date_format in ['%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%d/%m/%Y', '%m/%d/%y', '%d/%m/%y']:
		for time_format in ['%H:%M:%S%z', '%H:%M:%S.%f%z', '%I:%M:%S %p', '%I:%M:%S.%f %p']:
			datetime_format = date_format + ' ' + time_format
			try:
				datetime_obj = datetime.datetime.strptime(input, datetime_format)
				if not tz_aware:
					datetime_obj = datetime_obj.astimezone(tz).replace(tzinfo=None)
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
	if type(input) is str and re.match(r'-?\d+(\.0*)?$', input):
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
	if isinstance(input, decimal.Decimal):
		input = str(input)
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
	if type(input) is str and re.match(r'-?\d+(\.\d*)?$', input):
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
	if isinstance(input, decimal.Decimal):
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
string = common.convert_to_str(input)
* Converts None, booleans, integers, strings, and floats to strings
  Basically, str() but converts None to ''
"""
def convert_to_str(input):
	if input is None:
		return ''
	return str(input)

"""
boolean = common.is_email(input)
"""
def is_email(input):
	if type(input) is not str:
		return False
	return '@' in parseaddr(input)[1]

"""
boolean = common.is_hostname(input)
"""
def is_hostname(input):
	if type(input) is not str:
		return False
	if re.match(r"(?=.{1,253}$)(?!-)[A-Z\d-]{1,63}(?<!-)(\.(?!-)[A-Z\d-]{1,63}(?<!-)){1,126}\.?$", input, re.IGNORECASE):
		return True
	return False

"""
boolean = common.is_url(input)
"""
def is_url(input):
	if type(input) is not str:
		return False
	try:
		result = urllib.parse.urlparse(input)
		return all([result.scheme, result.netloc])
	except ValueError:
		return False
	return False

"""
boolean = common.is_uuid(input)
"""
def is_uuid(input):
	if type(input) is str and re.match(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', input):
		return True
	return False

"""
boolean = common.is_doc_id(input)
"""
def is_doc_id(input):
	if type(input) is str and re.search(r'^[a-zA-Z0-9]{22}$', input):
		return True
	return False

"""
boolean = common.is_hash_list(input)
"""
def is_hash_list(input):
	if type(input) is not list:
		return False
	for item in input:
		if type(item) is not dict:
			return False
	return True


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
	tz = datetime.timezone(datetime.timedelta(hours=0))
	now = datetime.datetime.now(tz)
	if format == 'time':
		return now.time()
	return now

def get_dt_future(days=0):
	tz = datetime.timezone(datetime.timedelta(hours=0))
	return datetime.datetime.now(tz) + datetime.timedelta(days=days)

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
		dt = convert_string_to_datetime(dt)
	else:
		dt = get_dt_now()
	return int(dt.timestamp())

def get_epoch_ms(dt=None):
	if dt:
		dt = convert_string_to_datetime(dt)
	else:
		dt = get_dt_now()
	return int(dt.timestamp() * 1000)


## Input checking

"""
output, errors = common.check_input(
	[
		[ "key_name", "str", True, 1, 16],
		[ "key_name", "str", ['value1', 'value2', ...]],
		[ "key_name2", "int", False, 1],
		[ "key_name3", "dict", [
			[ "key_name4", "str", True, 1, 16],
			[ "key_name5", "int", False, 1]
		]]
	],
	input_structure,
	required_or = [
		"first",
		"second",
		["third-a", "third-b"]
	]
)

* [ key_name, field_type, is_required, min_length, max_length ]

Field types:
	str
	int
	alphanumeric
	boolean
	dict
	list
	datetime
	date
	time
	email
	localpart
	hostname
	url
	uuid
	doc_id

"""
def check_input(field_list, body, allow_none=False, remove_none=False, process_query=False, required_or=None):
	output = {}
	errors = []
	field_map = {}
	if type(field_list) is not list:
		return {}, ["Invalid schema for check_input"]
	if not field_list:
		return {}, []
	for field in field_list:
		# Read args and set variables
		value_list = None
		children = None
		if type(field) is dict:
			key = field['name']
			ftype = field.get('type', 'str')
			required = field.get('required', False)
			min_length = field.get('min', 0)
			max_length = field.get('max', 0)
			if field.get('values'):
				value_list = field['values']
			if ftype in ['dict', 'list'] and field.get('children'):
				children = field['children']
		else:
			key = field[0]
			ftype = field[1]
			required = False
			value_list = None
			if len(field) >= 3:
				if type(field[2]) is bool:
					required = field[2]
				elif type(field[2]) is list:
					value_list = field[2]
			min_length = 0
			if len(field) >= 4:
				if type(field[3]) is int:
					min_length = field[3]
				elif ftype in ['dict', 'list']:
					children = field[3]
			max_length = 0
			if len(field) >= 5 and type(field[4]) is int:
				max_length = field[4]
		
		if ftype == 'int' and max_length == 0:
			max_length = 2147483647
		if ftype == 'bool':
			ftype = 'boolean'

		# Undo query lists into values
		if process_query:
			if key in body and type(body[key]) is list and len(body[key]) == 1 and ftype != 'list':
				body[key] = body[key][0]

		# Build map of fields
		field_map[key] = {
			"type": ftype,
			"required": required,
			"min_length": min_length,
			"max_length": max_length
		}
		if value_list:
			field_map[key] = value_list
		
		# Quoted key
		qkey = f"{quote_mark}{key}{quote_mark}"
		
		# Check required
		if key not in body:
			if (type(required) is list or required):
				errors.append("{} is required".format(qkey))
			continue
		if body[key] is None:
			if (type(required) is list or required):
				errors.append("{} is required".format(qkey))
				continue
		if type(required) is list:
			if body[key] not in required:
				errors.append("{} must be one of '{}'".format(qkey, "', '".join(required)))
		if remove_none and body[key] is None:
			continue

		# Verify type
		if not required and body[key] is None:
			pass
		elif ftype == "str":
			if type(body[key]) is not str:
				errors.append("{} must be a string".format(qkey))
				continue
		elif ftype == "int":
			if not is_int(body[key]):
				errors.append("{} must be an integer".format(qkey))
				continue
		elif ftype == "boolean":
			if convert_to_bool(body[key]) is None:
				errors.append("{} must be a boolean".format(qkey))
				continue
		elif ftype == "dict":
			if type(body[key]) is not dict:
				errors.append("{} must be a structure/hash".format(qkey))
				continue
		elif ftype == "list":
			if type(body[key]) is not list:
				errors.append("{} must be a list/array".format(qkey))
				continue
		elif ftype == "datetime":
			if convert_string_to_datetime(body[key]) is None:
				errors.append("{} must be a timestamp (datetime)".format(qkey))
				continue
		elif ftype == "date":
			if convert_string_to_date(body[key]) is None:
				errors.append("{} must be a date".format(qkey))
				continue
		elif ftype == "time":
			if convert_string_to_time(body[key]) is None:
				errors.append("{} must be a time".format(qkey))
				continue
		elif ftype == "alphanumeric":
			if type(body[key]) is not str or type(body[key]) is not int and not re.match(r'\w+$', str(body[key])):
				errors.append("{} must only be alphanumeric characters or underscores".format(qkey))
				continue
		elif ftype == "email":
			if not is_email(body[key]):
				errors.append("{} must be an email address".format(qkey))
				continue
		elif ftype == "localpart":
			if not is_email(f"{body[key]}@example.com"):
				errors.append("'{}' must be the local-part of an email address".format(key))
				continue
		elif ftype == "hostname":
			if not is_hostname(body[key]):
				errors.append("{} must be a hostname".format(qkey))
				continue
		elif ftype == "url":
			if not is_url(body[key]):
				errors.append("{} must be a url".format(qkey))
				continue
		elif ftype == "uuid":
			if not is_uuid(body[key]):
				errors.append("{} must be a uuid".format(qkey))
				continue
		elif ftype == "doc_id":
			if not is_doc_id(body[key]):
				errors.append("{} must be a doc_id".format(qkey))
				continue

		# Verify restrictions and set values
		if not required and body[key] is None:
			output[key] = None
		elif value_list and ftype in ['str', 'int']:
			if body[key] not in value_list:
				errors.append("{} should be one of '{}'.".format(qkey, "', '".join(value_list)))
				continue
			output[key] = str(body[key])
		elif ftype in ['str', 'alphanumeric', 'email', 'localpart', 'hostname', 'url']:
			if min_length and (body[key] is None or len(body[key]) < min_length):
				errors.append("{} should not be less than {} long".format(qkey, plural(min_length, 'character')))
				continue
			if body[key] is None or (max_length and len(body[key]) > max_length):
				errors.append("{} should not be greater than {} long".format(qkey, plural(max_length, 'character')))
				continue
			if allow_none and body[key] is None:
				output[key] = None
			else:
				output[key] = str(body[key])
		elif ftype == "int":
			if min_length and (body[key] is None or int(body[key]) < min_length):
				errors.append("{} should be greater than {}".format(qkey, min_length))
				continue
			if body[key] is None or (max_length and int(body[key]) > max_length):
				errors.append("{} should be less than {}".format(qkey, max_length))
				continue
			if allow_none and body[key] is None:
				output[key] = None
			else:
				output[key] = convert_to_int(body[key])
		elif allow_none and body[key] is None:
			output[key] = None
		elif ftype == "boolean":
			output[key] = convert_to_bool(body[key])
		elif ftype == "dict" or ftype == "list":
			if children:
				sub_output, sub_errors = check_input(children, body[key], allow_none=allow_none, remove_none=remove_none)
				if len(sub_output):
					output[key] = sub_output
				if len(sub_errors):
					errors.extend(sub_errors)
			else:
				output[key] = body[key]
		elif ftype == "datetime":
			output[key] = convert_string_to_datetime(body[key])
		elif ftype == "date":
			output[key] = convert_string_to_date(body[key])
		elif ftype == "time":
			output[key] = convert_string_to_time(body[key])
		elif ftype in ["uuid", "doc_id"]:
			output[key] = str(body[key])

	# process required_or
	if required_or and type(required_or) is list:
		found = False
		for field in required_or:
			# Handle a list of fields as AND
			if type(field) is list and len(field):
				found = True
				for subfield in field:
					if subfield not in field_map or subfield not in output:
						found = False
						break
					elif not output[subfield]:
						found = False
						break
			# Handle single field
			elif field in field_map and field in output:
				if field_map[field]['type'] == 'boolean':
					found = True
					break
				elif output[field]:
					found = True
					break
		if not found:
			message = conjunction(required_or, conj='or', quote="'")
			errors.append("One of the following is required: {}".format(message))
	return output, errors


# Serverless functions

## How is it called?
# Invoked using Lambda
def is_lambda() -> bool:
	return 'LAMBDA_TASK_ROOT' in os.environ

# Called as a BBEdit script
def is_bbedit() -> bool:
	return 'BBEDIT_PID' in os.environ

# Called as a script
def is_cli() -> bool:
	return not is_lambda()


## Where is it called? Helpful for knowing whether to use tunnels
# Running locally on a client machine
def is_local():
	if (is_cli() or is_bbedit() or os.environ.get('IS_LOCAL') == 'true') and 'SSH_CLIENT' not in os.environ:
		return True
	return False

# Running at AWS
def is_aws():
	return not is_local()



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
output_list = common.convert_dict_to_list(input_dict, case='lower')

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

	args = {}
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
	if type(response) is dict:
		if 'ResponseMetadata' in response:
			if response['ResponseMetadata'].get('HTTPStatusCode') >= 200 and response['ResponseMetadata'].get('HTTPStatusCode') < 300:
				return True
		elif 'statusCode' in response:
			if response.get('statusCode') >= 200 and response.get('statusCode') < 300:
				return True
	return False
