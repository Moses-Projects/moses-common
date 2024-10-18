#!/usr/bin/env python

import json
import os
import re
import sys
import urllib.parse

# import moses_common.__init__ as common


def parse(input_string=None, output='json'):
	print("here")
	if not input_string:
		return ""
	
	lines = input_string.split('\n')
	
	full = ''
	for line in lines:
		# Skip comment lines
		if re.match(r'\s*#', line):
			continue
		
		# URL decode line
		if re.match(r'%([a-fA-F0-9][a-fA-F0-9])', line) and re.match(r'\S+$', line):
			line = urllib.parse.unquote(line)
		
		# Remove trailing backslashes
		line = re.sub(r'\\+$', '', line)
		
		# Unescape double quotes
		if re.search(r'\\"', line) and not re.search(r'(?<!\\)"', line):
			line = re.sub(r'\\"', '"', line)
		
		# Amazon Ion ?
		if re.match(r'\s*\w+:', line):
			line = re.sub(r'^(\s*)(\w+):', r'\1"\2":', line)
			line = re.sub(r'(\{\{.*?\}\})', r'"\1"', line)
			line = re.sub(r':\s*(\d{4}-\d\d-\d\d.*?)(,|$)', r': "\1"\2', line)
		full += line + '\n'
	
	# Unescape newlines and tabs
	if re.match(r'^[\{\[]\\n', full):
		full = re.sub(r'\\n', '\n', full)
		full = re.sub(r'\\t', '\t', full)
	
	# Convert Python values
	full = re.sub(r':\s*(True|TRUE)', ': true', full)
	full = re.sub(r':\s*(False|FALSE)', ': false', full)
	full = re.sub(r':\s*(None|NONE)', ': null', full)
	
	# Determine quoting
	quote_type = 'double';
	single_count = re.findall(r"(')", full)
	double_count = re.findall(r'(")', full)
	
	# Process single quoted JSON
	if len(single_count) > len(double_count):
		# Escape double quotes
		full = re.sub(r'"', '\\"', full)
		
		# Keys
		full = re.sub(r"'([^']*)'(\s*:)", r'"\1"\2', full)
		
		# Values
		full = re.sub(r"([:\[\{,]\s*)'([^']*)'", r'\1"\2"', full)
	
	# Convert classes
	full = re.sub(r':\s*(\<.*?\>)', r': "\1"', full)
	
	# datetime.datetime(2024, 8, 6, 13, 4, 14, 833579, tzinfo=tzfile('/usr/share/zoneinfo/US/Central'))
	full = re.sub(r"tzfile\('.*?/(\w+/\w+)'\)", r'\1', full)
	full = re.sub(r'datetime.\w+(\(.*?(utc|tzinfo=\w+)\(\)\))', r'"\1"', full)
	full = re.sub(r'datetime.\w+(\(.*?\))', r'"\1"', full)
	
	python_object = json.loads(full)
	return json.dumps(python_object, sort_keys=False, indent="\t"))
