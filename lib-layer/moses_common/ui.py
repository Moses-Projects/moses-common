# print("Loaded UI module")

# The UI module handles terminal-based interfaces
# Converted from the Perl SitemasonPl::IO module

import getopt
import re
import os
import sys

import moses_common.__init__ as common


"""
import moses_common.ui
"""

class Interface:
	"""
	ui = moses_common.ui.Interface()
	"""
	def __init__(self, use_slack_format=False, force_whitespace=False, usage_message=None):
		
		self.use_slack_format = common.convert_to_bool(use_slack_format) or False
		
		self.force_whitespace = common.convert_to_bool(force_whitespace) or False
		
		self._usage_message = usage_message
		
		self._colors = self._get_term_color_numbers()
		
		self._args = {}
		self._opts = {}
	
	@property
	def supports_color(self):
		if os.environ.get('TERM') and os.environ.get('TERM') not in ['dumb', 'tty']:
			return True

	@property
	def is_person(self):
		if self.supports_color or os.environ.get('SSH_AUTH_SOCK'):
			return True
	
	@property
	def is_aws_lambda(self):
		if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
			return True
	
	
	### Arg handing
	"""
	arguments, options = self.get_options({
		"args": [ {
			"name": "function",
			"label": "Lambda function",
			"required": True
		}, {
			"name": "environment",
			"label": "Environment",
			"values": ["dev", "prod"],
			"required": True
		}, {
			"name": "file",
			"label": "JSON file",
			"type": "file",
			"required": True
		}, {
			"name": "files",
			"label": "Image files",
			"type": "glob",
			"required": True
		} ],
		"options": [ {
			"short": "v",
			"long": "verbose"
		}, {
			"short": "f",
			"long": "file",
			"type": "file"
		} ]
	})
	"""
	def get_options(self, params={}):
		# Prep options
		short_opts = ""
		long_opts = []
		if self._usage_message:
			short_opts = "h"
			long_opts.append("help")
		if 'options' in params:
			for opt in params['options']:
				sa = ''
				la = ''
				if 'type' in opt and opt['type'] in ['input', 'file']:
					sa = ':'
					la = '='
				if 'short' in opt:
					short_opts += opt['short'] + sa
					if 'type' not in opt or opt['type'] == 'bool':
						self._opts[opt['short']] = False
					else:
						self._opts[opt['short']] = None
				if 'long' in opt:
					long_opts.append(opt['long'] + la)
					if 'type' not in opt or opt['type'] == 'bool':
						self._opts[opt['long']] = False
					else:
						self._opts[opt['long']] = None
		
		# Parse options and arguments
		try:
			opts, args = getopt.gnu_getopt(sys.argv[1:], short_opts, long_opts)
		except getopt.GetoptError as err:
			error = str(err)
			self.error(error.capitalize())  # will print something like "option -a not recognized"
			self.usage()
			sys.exit(2)
		
		# Handle options
		for o, a in opts:
			if o in ("-h", "--help"):
				self.usage()
				sys.exit()
			elif 'options' in params:
				for opt in params['options']:
					if ('short' in opt and o == '-' + opt['short']) or ('long' in opt and o == '--' + opt['long']):
						if 'type' not in opt or opt['type'] == 'bool':
							a = True
						if opt.get('type') == 'file':
							if not os.path.isfile(a):
								self.error("File '{}' not found".format(a))
								sys.exit(2)
						if 'short' in opt:
							self._opts[opt['short']] = a
						if 'long' in opt:
							self._opts[opt['long']] = a
		
		if 'options' in params:
			for param in params['options']:
				label = param.get('long')
				if 'label' in param:
					label = param['label']
				
				if 'default' in param and not self._opts[param['long']]:
					self._opts[param['short']] = param['default']
					self._opts[param['long']] = param['default']
				if param.get('type') == 'input' and type(param.get('values')) is list:
					if self._opts[param['long']] not in param['values']:
						self.error("Option '{}' must be one of '{}'".format(label, "', '".join(param['values'])))
						self.usage()
						sys.exit(2)
				
		# Handle arguments
		if 'args' in params:
			for param in params['args']:
				label = param['name']
				if 'label' in param:
					label = param['label']
				
				if 'required' in param and param['required'] and not len(args):
					self.error(f"Argument '{label}' is required")
					self.usage()
					sys.exit(2)
				
				if len(args):
					value = args.pop(0)
					if 'values' in param and type(param['values']) is list:
						if value not in param['values']:
							self.error("Argument '{}' must be one of '{}'".format(label, "', '".join(param['values'])))
							self.usage()
							sys.exit(2)
					if 'type' in param and param['type'] == 'file':
						if not os.path.isfile(value):
							self.error("File '{}' not found".format(value))
							sys.exit(2)
					elif 'type' in param and param['type'] == 'glob':
						if not os.path.isfile(value):
							self.error("File '{}' not found".format(value))
							sys.exit(2)
						value = [value]
						for i in range(len(args)):
							value.append(args.pop(0))
					self._args[param['name']] = value
				else:
					self._args[param['name']] = None
			self._args['args'] = args
		
		return self._args, self._opts

	"""
	inputs = ui.resolve_inputs(field_list, file)
	"""
	def resolve_inputs(self, field_list, file=None):
		inputs = []
		if file:
			inputs = common.read_csv(file)
			if not len(inputs):
				self.error("No records found in CSV '{}'".format(opts['file']))
		else:
			input = {}
			for field in field_list:
				if len(self._args['args']):
					input[field] = self._args['args'].pop(0)
				else:
					self.error("Missing '{}' argument".format(field))
					self.usage()
			inputs.append(input)
		return inputs


	
	
	
	### Output formatting
	
	# https://en.wikipedia.org/wiki/ANSI_escape_code
	# https://en.wikipedia.org/wiki/Web_colors
	def _get_term_color_numbers(self):
		colors = self._get_term_color_number_list()
		color_hash = {}
		for color in colors:
			color_hash[color['name']] = color['num']
		return color_hash
	
	def _get_term_color_number_list(self):
		return [
			{ "name": "reset",			"num": 0 },
			{ "name": "default",		"num": 39 },
			{ "name": "default_bg",		"num": 49 },
		
			{ "name": "bold",			"num": 1 },	# reset 21 # yes
			{ "name": "faint",			"num": 2 },	# reset 22 # yes
			{ "name": "italic",			"num": 3 },	# reset 23
			{ "name": "underline",		"num": 4 },	# reset 24 # yes
			{ "name": "blink",			"num": 5 },	# reset 25 # yes
			{ "name": "rapid",			"num": 6 },	# reset 26
			{ "name": "inverse",		"num": 7 },	# reset 27 # yes
			{ "name": "conceal",		"num": 8 },	# reset 28 # yes
			{ "name": "crossed",		"num": 9 },	# reset 29
		
			{ "name": "white",			"num": 97 },
			{ "name": "silver",			"num": 37 },
			{ "name": "gray",			"num": 90 },
			{ "name": "black",			"num": 30 },
		
			{ "name": "red",			"num": 91 },
			{ "name": "maroon",			"num": 31 },
			{ "name": "yellow",			"num": 93 },
			{ "name": "olive",			"num": 33 },
			{ "name": "lime",			"num": 92 },
			{ "name": "green",			"num": 32 },
			{ "name": "cyan",			"num": 96 },
			{ "name": "teal",			"num": 36 },
			{ "name": "blue",			"num": 94 },
			{ "name": "azure",			"num": 34 },
			{ "name": "pink",			"num": 95 },
			{ "name": "magenta",		"num": 35 },
		
			{ "name": "white_bg",		"num": 107 },
			{ "name": "silver_bg",		"num": 47 },
			{ "name": "gray_bg",		"num": 100 },
			{ "name": "black_bg",		"num": 40 },
		
			{ "name": "red_bg",			"num": 101 },
			{ "name": "maroon_bg",		"num": 41 },
			{ "name": "yellow_bg",		"num": 103 },
			{ "name": "olive_bg",		"num": 43 },
			{ "name": "lime_bg",		"num": 102 },
			{ "name": "green_bg",		"num": 42 },
			{ "name": "cyan_bg",		"num": 106 },
			{ "name": "teal_bg",		"num": 46 },
			{ "name": "blue_bg",		"num": 104 },
			{ "name": "azure_bg",		"num": 44 },
			{ "name": "pink_bg",		"num": 105 },
			{ "name": "magenta_bg",		"num": 45 },
		
			{ "name": "reset_bg",		"num": 49 }
		]
	
	def print_term_colors(self):
		colors = self._get_term_color_number_list()
		e = self.get_term_color('reset')
		for ind in range(3, (len(colors)-1)):
			color = colors[ind]
			color_num = str(color['num'])
			if ind < 12:
				name = color['name']
				ds, de = self.get_term_color(name)
				print(f"{name:14s}: {ds:s}{name:s}{de:s}")
			else:
				name = 'default'
				ds, de = self.get_term_color(name)
				bs, be = self.get_term_color([name, 'bold'])
				fs, fe = self.get_term_color([name, 'faint'])
				ist, ie = self.get_term_color([name, 'inverse'])
				if ind == 12:
					print("")
					header = f"{' ':14s}  {'default':14s} {'bold':14s} {'faint':14s} {'inverse':14s}"
					self.header(header)
					print(f"{name:14s}: {ds:s}{name:14s}{de:s} {bs:s}{name:14s}{be:s} {fs:s}{name:14s}{fe:s} {ist:s}{name:14s}{ie:s}")
				elif ind == 28:
					print("")
				name = color['name']
				ds, de = self.get_term_color(name)
				bs, be = self.get_term_color([name, 'bold'])
				fs, fe = self.get_term_color([name, 'faint'])
				ist, ie = self.get_term_color([name, 'inverse'])
				print(f"{name:14s}: {ds:s}{name:14s}{de:s} {bs:s}{name:14s}{be:s} {fs:s}{name:14s}{fe:s} {ist:s}{name:14s}{ie:s}")
	
	def print_sample_sections(self):
		self.info("This is info")
		self.warning("This is a warning\n  Line 2")
		self.error("This is an error\n  Line 2")
		self.title("This is a title")
		self.header("This is a header")
		self.header2("This is a header2")
		self.body("This is a body")
		self.body("> This is a quote")
		self.success("This is success")
		self.dry_run("This is a dry run")
		self.verbose("This is verbose")
	
	
	# color_code = ui.get_term_color(color_name)
	def get_term_color(self, names):
		if not self.supports_color:
			return '', ''
		
		if type(names) is str:
			names = [names]
		color_nums = []
		for name in names:
			if name in self._colors:
				color_nums.append(str(self._colors[name]))
		if not len(color_nums):
			return '', ''
		
		code = "\033"
		start = "{}[{}m".format(code, ';'.join(color_nums))
		
		end = code + "[0m"
		if len(names) == 1:
			if self._colors[names[0]] >= 2 and self._colors[names[0]] <= 9:
				end = "{}[{}m".format(code, str(self._colors[name]+20))
			elif re.search(r'_bg$', names[0]):
				end = "{}[{}m".format(code, str(self._colors['reset_bg']))
		
		return start, end
	
	# formatted_string = ui.format_text(text, colors)
	# formatted_string = ui.format_text(text, 'blue')
	# formatted_string = ui.format_text(text, ['blue', 'white_bg', 'bold'])
	def format_text(self, text, colors, quote=None):
		text = str(text)
		start, end = self.get_term_color(colors)
		
		quote_string = ''
		if quote:
			quote_string = self.make_quote(quote)
		
		output = []
		lines = text.split("\n")
		for line in lines:
			output.append("{}{}{}{}".format(quote_string, start, line, end))
		return "\n".join(output)
	
	def convert_slack_to_ansi(self, text=None):
		if not text or not len(str(text)):
			return ''
		if not self.use_slack_format:
			return text
		
	# 	$text =~ s/(?:^|(?<=\s))\*(\S.*?)\*/\e[1m$1\e[21m/g;
		text = re.sub(r'(?:^|(?<=\s))\*(\S.*?)\*', lambda m: self.format_text(m.group(1), 'bold'), str(text))
		text = re.sub(r'(?:^|(?<=\s))\_(\S.*?)\_', lambda m: self.format_text(m.group(1), 'underline'), text)
		text = re.sub(r'(?:^|(?<=\s))~(\S.*?)~', lambda m: self.format_text(m.group(1), 'inverse'), text)
		text = re.sub(r'(?:^|(?<=\s))`(\S.*?)`', lambda m: self.format_text(m.group(1), ['red', 'silver_bg']), text)
		text = re.sub(r'^> ', lambda m: self.make_quote(), text)
# 		$text =~ s/^>/$self->make_quote('silver_bg')/egm;
		return text
	
	def make_quote(self, color_name='silver_bg'):
		if not re.search(r'_bg$', color_name) or not self.supports_color:
			return '| '
		indent = self.format_text(' ', color_name)
		return indent + ' '
	
	def process_whitespace(self, text):
		if self.is_aws_lambda and self.force_whitespace and type(text) is str:
			return re.sub(r'  ', '. ', text)
		return text
	
	def bold(self, text=None):
		if not text or not len(str(text)):
			return ''
		return self.format_text(text, 'bold')
	
	def body(self, text=None):
		text = self.process_whitespace(text)
		text = self.convert_slack_to_ansi(text)
		if not len(text):
			return
		print(text)
	
	def title(self, text=None):
		text = self.convert_slack_to_ansi(text)
		if not len(text):
			return
		print(self.format_text(f' {text:s} ', ['blue', 'bold', 'inverse']))
	
	def header(self, text=None):
		text = self.convert_slack_to_ansi(text)
		if not len(text):
			return
		print(self.format_text(text, ['blue', 'bold', 'underline']))
	
	def header2(self, text=None):
		text = self.convert_slack_to_ansi(text)
		if not len(text):
			return
		print(self.format_text(text, ['bold', 'underline']))
	
	def success(self, text=None):
		text = self.convert_slack_to_ansi(text)
		if not len(text):
			return
		print(self.format_text(text, ['green', 'bold']))
	
	def dry_run(self, text=None):
		text = self.convert_slack_to_ansi(text)
		if not len(text):
			return
		print(self.format_text(text, ['silver'], quote='silver_bg'))
	
	def verbose(self, text=None):
		text = self.convert_slack_to_ansi(text)
		if not len(text):
			return
		print(self.format_text(text, ['gray'], quote='gray_bg'))
	
	# syslog severity 6
	def info(self, text=None):
		text = self.convert_slack_to_ansi(text)
		if not len(text):
			return
		print(self.format_text(text, ['gray'], quote='gray_bg'))
	
	# syslog severity 4
	def warning(self, text=None):
		text = self.convert_slack_to_ansi(text)
		if not len(text):
			return
		print(self.format_text(text, ['olive', 'bold'], quote='olive_bg'))
	
	# syslog severity 3
	def error(self, text=None):
		text = self.convert_slack_to_ansi(text)
		if not len(text):
			return
		print(self.format_text('ERROR: ' + text, ['maroon', 'bold'], quote='maroon_bg'))
	
	def usage(self):
		if not self._usage_message:
			return
		self.info(self._usage_message)
	
	def pretty(self, input, label=None, colorize=True):
		if label:
			label = self.convert_slack_to_ansi(label)
			if len(label):
				print(label)
		if self.is_person and (type(input) is dict or type(input) is list):
			input = common.make_json(input, pretty_print=True)
			if self.supports_color and colorize:
				input = re.sub(r'(".*?"):', lambda m: self.format_text(m.group(1), 'blue') + ':', str(input))
				input = re.sub(r': (".*?")', lambda m: ': ' + self.format_text(m.group(1), 'red'), str(input))
				input = re.sub(r': ([0-9.-]+)', lambda m: ': ' + self.format_text(m.group(1), 'maroon'), str(input))
				input = re.sub(r': (true|false|null)', lambda m: ': ' + self.format_text(m.group(1), 'magenta'), str(input))
		print(input)

	def format_table(self, records, fields=None, frame=False):
		records = common.to_list(records)
		if not fields:
			fields = list(records.keys())
		divider = " "
		if frame:
			divider = "|"
		
		## Find field lengths and types
		field_info = {}
		for field in fields:
			field_info[field] = {
				"length": len(field),
				"type": "numeric"
			}
		for record in records:
			for field in fields:
				length = len(str(record.get(field)))
				if length > field_info[field]['length']:
					field_info[field]['length'] = length
				if type(record.get(field)) is str:
					field_info[field]['type'] = 'string'
		
		## Assemble output
		# Add header
		output = divider
		cnt = 0
		for field in fields:
			cnt += 1
			if cnt > 1:
				output += divider
			
			length = field_info[field]['length']
			if field_info[field]['type'] == 'numeric':
				output += f"{field:>{length}}"
			else:
				output += f"{field:<{length}}"
		output += divider
		output = self.format_text(output, 'underline')
		output += "\n"
		
		# Add records
		while records:
			record = records.pop(0)
			output += divider
			cnt = 0
			for field in fields:
				cnt += 1
				if cnt > 1:
					output += divider
				length = field_info[field]['length']
				value = record.get(field, '')
				output += f"{value:{length}}"
			output += divider
			if records:
				output += "\n"
		return output

