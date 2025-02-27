# print("Loaded language:accent module")

import json
import os
import random
import re

import moses_common.__init__ as common
import moses_common.language.__init__

phonemes = None
dictionary = None
language_def = {}

class Accent:
	"""
	import moses_common.language.accent
	
	speech = moses_common.language.accent.Accent(dialect, args, ui=ui, log_level=log_level, dry_run=dry_run)
	"""
	def __init__(self, dialect, args, ui=None, log_level=5, dry_run=False):
		self.dry_run = dry_run
		self.log_level = log_level
		self.ui = ui or moses_common.ui.Interface()
		
		self.dialect = dialect
		self.args = args
		self._definitions = None
		
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def phonemes(self):
		global phonemes
		if not phonemes:
			phonemes = self.read_phonemes()
		return phonemes
	
	@property
	def dictionary(self):
		global dictionary
		if not dictionary:
			dictionary = self.read_cmudict()
		return dictionary
	
	def find_data_file(self, name):
		base_dir = os.path.dirname(moses_common.language.__init__.__file__)
		path = f"{base_dir}/data/{name}"
		if not os.path.isfile(path):
			return None
		return path
	
	def read_phonemes(self):
		contents = {}
		path = self.find_data_file('phonemes.json')
		if not path:
			return None
		with open(path) as f:
			contents = json.loads(f.read())
		f.closed
		return contents
	
	def read_cmudict(self):
		path = self.find_data_file('cmudict-0.7b.txt')
		if not path:
			return None
		
		cmudict = {}
		match = re.compile('', re.IGNORECASE)
		with open(path) as f:
			for line in f:
				if re.match(r'[^A-Za-z]', line):
					continue
				pair = re.split(r'  ', line)
				if re.search(r'X', pair[0]):
					pair[1] = re.sub(r'K S', 'KS', pair[1], count=0)
					pair[1] = re.sub(r'G Z', 'GZ', pair[1], count=0)
				cmudict[pair[0]] = pair[1].rstrip()
		f.closed
		return cmudict
	
	def get_language_def(self, code):
		global language_def
		code = re.sub(r'_\w+$', '', code)
		if code in language_def:
			return language_def[code]
		print(f"Reading {code}")
		path = self.find_data_file(f"lang/{code}.yml")
		if not path:
			return None
		lang_def = common.read_file(path)
		if not lang_def:
			return None
		language_def[code] = lang_def
		return language_def[code]
	
	@property
	def definitions(self):
		if not self._definitions:
			if self.dialect == 'pirate':
				self._definitions = {
					"glottal_stop":[
						{
							"w":	"certain",
							"p":	"cer'ain"
						}, {
							"w":	"fountain",
							"p":	"foun'ain"
						}, {
							"w":	"important",
							"p":	"impor'ant"
						}, {
							"w":	"mountain",
							"p":	"moun'ain"
						}
					],
					"hard_th": [
						{
							"w":	"this",
							"p":	"dhis"
						}, {
							"w":	"that",
							"p":	"dhat"
						}
					],
					"dropped_g": [
						{
							"r":	"([a-z]*[aeiouy][a-z]*)(ing)()",
							"p":	"in'"
						}
					]
				}
				
# 				{ "section": "Strong R, currr" },
# 				{ "section": "Elongated vowels - gooold, seeea" },
# 				{ "section": "Dropped H at beginning - 'ello" }
		return self._definitions
	
	def convert_text_to_phonetic(self, code, text):
		lang_def = self.get_language_def(code)
		if not lang_def:
			return text
		
		words = self.split_text_into_words(text)
		
		phonetics = []
		for word in words:
			clean_word = common.convert_to_ascii(word)
			parts = re.match(r"([^a-z']*)([a-z']+)([^a-z']*)$", clean_word, re.I)
			if not parts:
				phonetics.append(word)
				continue
			pre = parts.group(1)
			bare_word = parts.group(2)
			post = parts.group(3)
			
			phonetic, phoneme_string = self.convert_word_to_phonetic(code, bare_word)
			if self.log_level >= 6:
				print(f"{pre}{phoneme_string}{post}")
			phonetics.append(f"{pre}{phonetic}{post}")
		return ' '.join(phonetics)
	
	def split_text_into_words(self, text):
		words = re.split(r'\s+', text)
		return words
	
	def convert_word_to_phonemes(self, word):
		if self.is_phoneme_string(word):
			return word
		phoneme_string = self.dictionary.get(word.upper())
		return phoneme_string
	
	def is_phoneme_string(self, word):
		if re.match(r'((AA|AE|AH|AO|AW|AY|EH|ER|EY|IH|IY|OW|OY|UH|UW)[0-2]?|(B|CH|D|DH|F|G|HH|JH|K|L|M|N|NG|P|R|S|SH|T|TH|V|W|Y|Z|ZH| ))+$', word):
			return True
		return False
	
	def get_vowel_count(self, word):
		phoneme_string = self.convert_word_to_phonemes(word)
		matches = re.findall(r'((?:AA|AE|AH|AO|AW|AY|EH|ER|EY|IH|IY|OW|OY|UH|UW)[0-2]?)', word)
# 		print("matches {}: {}".format(type(matches), matches))
		return len(matches)
	
	def convert_word_to_phonetic(self, code, word):
		lang_def = self.get_language_def(code)
		if not lang_def:
			return word
		
		phoneme_string = self.convert_word_to_phonemes(word)
		if not phoneme_string:
			return word, ''
		phonemes = phoneme_string.split(' ')
# 		print("phonemes {}: {}".format(type(phonemes), phonemes))
		vowel_count = self.get_vowel_count(phoneme_string)
		phonetic_word = ''
		for phoneme in phonemes:
			phonetic_word += self.convert_phoneme_to_phonetic(code, phoneme, vowel_count=vowel_count)
		return phonetic_word, phoneme_string
	
	"""
	It ought to be odd that cows eat oats and hide toys.
	Ed ate cheese and hurt his key knee.
	We ping fee huts and the green hood just thanks very like me.
	She reads sea yield and don't seizure.
	"""
	def convert_phoneme_to_phonetic(self, code, phoneme, vowel_count=1):
		lang_def = self.get_language_def(code)
		if not lang_def:
			return None
		
		# Remove accent in single vowel words
# 		if vowel_count <= 1:
# 			phoneme = re.sub(r'[12]$', '0', phoneme)
		
		# As is
		if phoneme in lang_def['phonetics']:
			return lang_def['phonetics'][phoneme]
		
		# Fill in missing accented vowels
		if re.search(r'[12]$', phoneme):
			# Fill in 1 and 2 if not there
			first, second, accent = list(phoneme)
			phoneme = f"{first}{second}0"
			if phoneme in lang_def['phonetics']:
				phonetic = lang_def['phonetics'][phoneme]
				# If no secondary accent, use no accent
				if accent == '2':
					return phonetic
				# If no primary accent, add accent to no accent
				if accent == '1':
					accent_map = {
						"a": "á",
						"e": "é",
						"i": "í",
						"o": "ó",
						"u": "ú"
					}
					parts = list(phonetic)
					if parts[0] in accent_map:
						parts[0] = accent_map[parts[0]]
						return ''.join(parts)
					return phonetic
		phoneme = re.sub(r'\d$', '', phoneme)
		return phoneme.lower()
	
	
	
	
	
	def convert_word_into_phonetics(self, text):
		cmudict = self.read_cmudict()
		phoneme_library = self.read_phonemes()
		
		re_word = re.compile("([a-z][a-z']*)([^a-z]+|$)", re.IGNORECASE)
		while len(input):
			if self.log_level >= 7:
				print("start:", input)
			segment = re_word.match(input)
			original = segment.group(1)
			space = segment.group(2)
			if self.log_level >= 7:
				print("parts: '" + original + "', '" + space + "'")
			input = re_word.sub('', input, count=1)
			if self.log_level >= 7:
				print("end:", input)
			
			if original.upper() not in cmudict:
				word = self.backup_plan(accent, original)
				
				if output['phonetic']:
					output['phonetic'] += " "
				output['phonetic'] += word['ph'] + space
				output['mild'] += common.match_capitalization(original, word['m']) + space
				output['medium'] += common.match_capitalization(original, word['n']) + space
				output['strong'] += common.match_capitalization(original, word['x']) + space
				continue;
			
			phonetic = cmudict[original.upper()]
			word_split = self.split_word(original, phonetic, phoneme_library)
			phonemes = word_split['phonemes']
			letters = word_split['letters']
		
			if self.log_level >= 6:
				print("letters:", letters)
				print("phonemes:", phonemes)
		
			for i in range(0, len(phonemes)):
				phoneme = phonemes[i]
				letter = letters[i]
				is_first = False
				is_last = False
				if i == 0:
					is_first = True
				if i >= (len(phonemes)-1):
					is_last = True
			
				ltr = self.sub_phoneme(accent, output, letter, phoneme, is_first, is_last)
				
				if output['phonetic']:
					output['phonetic'] += " "
				output['phonetic'] += ltr['ph']
				output['mild'] += common.match_capitalization(letter, ltr['m'])
				output['medium'] += common.match_capitalization(letter, ltr['n'])
				output['strong'] += common.match_capitalization(letter, ltr['x'])
				
			output['phonetic'] += space
			output['mild'] += space
			output['medium'] += space
			output['strong'] += space
	
	
	
	
	
	
	
	
	
	
	
	
	def apply(self, word):
		for name, def_list in self.definitions.items():
			if self.args.get(name) or self.args.get('all'):
				for sub_def in def_list:
					if 'w' in sub_def and sub_def['w'] == word:
						return sub_def['p']
					if 'r' in sub_def and re.search(sub_def['r'], word, re.I):
						return sub_def['p']
		return word
	
	def apply_accent(self, accent, input):
		output = {
			'phonetic': "",
			'mild': "",
			'medium': "",
			'strong': ""
		}
		
		cmudict = self.read_cmudict()
		phoneme_library = self.read_phonemes()
		
		re_word = re.compile("([a-z][a-z']*)([^a-z]+|$)", re.IGNORECASE)
		while len(input):
			if self.log_level >= 7:
				print("start:", input)
			segment = re_word.match(input)
			original = segment.group(1)
			space = segment.group(2)
			if self.log_level >= 7:
				print("parts: '" + original + "', '" + space + "'")
			input = re_word.sub('', input, count=1)
			if self.log_level >= 7:
				print("end:", input)
			
			if original.upper() not in cmudict:
				word = self.backup_plan(accent, original)
				
				if output['phonetic']:
					output['phonetic'] += " "
				output['phonetic'] += word['ph'] + space
				output['mild'] += common.match_capitalization(original, word['m']) + space
				output['medium'] += common.match_capitalization(original, word['n']) + space
				output['strong'] += common.match_capitalization(original, word['x']) + space
				continue;
			
			phonetic = cmudict[original.upper()]
			word_split = self.split_word(original, phonetic, phoneme_library)
			phonemes = word_split['phonemes']
			letters = word_split['letters']
		
			if self.log_level >= 6:
				print("letters:", letters)
				print("phonemes:", phonemes)
		
			for i in range(0, len(phonemes)):
				phoneme = phonemes[i]
				letter = letters[i]
				is_first = False
				is_last = False
				if i == 0:
					is_first = True
				if i >= (len(phonemes)-1):
					is_last = True
			
				ltr = self.sub_phoneme(accent, output, letter, phoneme, is_first, is_last)
				
				if output['phonetic']:
					output['phonetic'] += " "
				output['phonetic'] += ltr['ph']
				output['mild'] += common.match_capitalization(letter, ltr['m'])
				output['medium'] += common.match_capitalization(letter, ltr['n'])
				output['strong'] += common.match_capitalization(letter, ltr['x'])
				
			output['phonetic'] += space
			output['mild'] += space
			output['medium'] += space
			output['strong'] += space
			
		output['mild'] = re.sub(r'\bze\b', 'zee', output['mild'])
		output['medium'] = re.sub(r'\bze\b', 'zee', output['medium'])
		output['strong'] = re.sub(r'\bze\b', 'zee', output['strong'])
		output['strong'] = re.sub(r'\bpe\b', 'pee', output['strong'])
		output['medium'] = re.sub(r'\'o\b', '\'oo', output['medium'])
		output['strong'] = re.sub(r'\'o\b', '\'oo', output['strong'])
		return output
	
	
	def sub_phoneme(self, accent, output, letter, phoneme, is_first=False, is_last=False):
		ltr = {
			'ph': phoneme,
			'm': letter,
			'n': letter,
			'x': letter
		}
		
		if accent == 'pirate':
			"""
			Strong R - double r's
			Elongated vowels - gold -> gooold, sea -> seeea - maybe end of sentence?
			Dropped G in gerunds - ing -> in'
			TH as hard D or T - that -> dat
			Glottal stop - T in middle of words - bottle -> bah'ul?
			Dropped H at beginning of words - 
			"""
			pass
		elif accent == 'french':
			if is_last and phoneme == 'ER':
				ltr = {
					'ph': 'AH HH',
					'm': letter,
					'n': re.sub(r'r+', 'h', letter.lower()),
					'x': re.sub(r'r+', 'h', letter.lower())
				}
			elif phoneme == 'ER':
				ltr = {
					'ph': 'AH W',
					'm': letter,
					'n': re.sub(r'r+', 'w', letter.lower()),
					'x': re.sub(r'r+', 'w', letter.lower())
				}
			elif is_last and phoneme == 'R':
				ltr = {
					'ph': 'HH',
					'm': letter,
					'n': re.sub(r'r+', 'h', letter.lower()),
					'x': re.sub(r'r+', 'h', letter.lower())
				}
			elif phoneme == 'R':
				ltr = {
					'ph': 'W',
					'm': letter,
					'n': re.sub(r'r+', 'w', letter.lower()),
					'x': re.sub(r'r+', 'w', letter.lower())
				}
			elif phoneme == 'P':
				ltr = {
					'ph': "(pb)",
					'm': letter,
					'n': re.sub(r'p', 'b', letter.lower()),
					'x': re.sub(r'p', 'b', letter.lower())
				}
			elif phoneme == 'T':
				ltr = {
					'ph': "(td)",
					'm': letter,
					'n': re.sub(r't', 'd', letter.lower()),
					'x': re.sub(r't', 'd', letter.lower())
				}
			elif phoneme == 'K':
				ltr = {
					'ph': "(kg)",
					'm': letter,
					'n': 'g',
					'x': 'g'
				}
			elif phoneme == 'KS':
				ltr = {
					'ph': "(kg)S",
					'm': letter,
					'n': 'gs',
					'x': 'gs'
				}
			elif phoneme == 'B':
				ltr = {
					'ph': "(bp)",
					'm': letter,
					'n': letter,
					'x': re.sub(r'b', 'p', letter.lower())
				}
			elif phoneme == 'D':
				ltr = {
					'ph': "(dt)",
					'm': letter,
					'n': letter,
					'x': re.sub(r'd', 't', letter.lower())
				}
			elif phoneme == 'G':
				ltr = {
					'ph': "(gk)",
					'm': letter,
					'n': letter,
					'x': re.sub(r'g', 'k', letter.lower())
				}
			elif phoneme == 'GZ':
				ltr = {
					'ph': "(gk)S",
					'm': letter,
					'n': letter,
					'x': 'ks'
				}
			elif is_first and phoneme == 'TH':
				ltr = {
					'ph': 'S',
					'm': re.sub(r'th', 's', letter.lower()),
					'n': re.sub(r'th', 's', letter.lower()),
					'x': re.sub(r'th', 's', letter.lower())
				}
			elif is_last and phoneme == 'TH':
				if re.search(r'EH *$', output['phonetic']) and re.search(r'ea$', output['mild']):
					output['mild'] = re.sub(r'ea$', 'e', output['mild'])
					output['medium'] = re.sub(r'ea$', 'e', output['medium'])
					output['strong'] = re.sub(r'ea$', 'e', output['strong'])
				ltr = {
					'ph': 'S',
					'm': re.sub(r'th', 's', letter.lower()),
					'n': re.sub(r'th', 's', letter.lower()),
					'x': re.sub(r'th', 's', letter.lower())
				}
			elif phoneme == 'TH':
				ltr = {
					'ph': 'F',
					'm': re.sub(r'th', 'f', letter.lower()),
					'n': re.sub(r'th', 'f', letter.lower()),
					'x': re.sub(r'th', 'f', letter.lower())
				}
			elif (is_first or is_last) and phoneme == 'DH':
				ltr = {
					'ph': 'Z',
					'm': re.sub(r'th', 'z', letter.lower()),
					'n': re.sub(r'th', 'z', letter.lower()),
					'x': re.sub(r'th', 'z', letter.lower())
				}
			elif phoneme == 'DH':
				ltr = {
					'ph': 'V',
					'm': re.sub(r'th', 'v', letter.lower()),
					'n': re.sub(r'th', 'v', letter.lower()),
					'x': re.sub(r'th', 'v', letter.lower())
				}
			elif is_first and phoneme == 'HH':
				ltr = {
					'ph': "'",
					'm': letter,
					'n': "'",
					'x': "'"
				}
			elif is_last and phoneme == 'NG':
				ltr = {
					'ph': "NG G",
					'm': letter,
					'n': "n-g",
					'x': "n-g"
				}
			elif phoneme == 'CH':
				ltr = {
					'ph': 'SH',
					'm': re.sub(r'ch', 'sh', letter.lower()),
					'n': re.sub(r'ch', 'sh', letter.lower()),
					'x': re.sub(r'ch', 'sh', letter.lower())
				}
			elif phoneme == 'JH':
				ltr = {
					'ph': 'ZH',
					'm': letter,
					'n': re.sub(r'[gj]', 'zh', letter.lower()),
					'x': re.sub(r'[gj]', 'zh', letter.lower())
				}
			elif is_last and phoneme == 'Z':
				ltr = {
					'ph': 'S',
					'm': letter,
					'n': re.sub(r's$', '-s', letter.lower()),
					'x': re.sub(r's$', '-s', letter.lower())
				}
			elif is_last and phoneme == 'S' and re.search(r'([a-hj-np-z])', output['mild'].lower()):
				ltr = {
					'ph': "'",
					'm': letter,
					'n': re.sub(r'se?$', '', letter.lower()),
					'x': re.sub(r'se?$', '', letter.lower()),
				}
		return ltr
	
	
	def backup_plan(self, accent, original):
		word = {
			'ph': original.lower(),
			'm': original.lower(),
			'n': original.lower(),
			'x': original.lower()
		}
		
		if accent == 'french':
			if re.match(r'^th', original.lower()):
				word['ph'] = re.sub(r'^th', 'Z', word['ph'])
				word['m'] = re.sub(r'^th', 'z', word['m'])
				word['n'] = re.sub(r'^th', 'z', word['n'])
				word['x'] = re.sub(r'^th', 'z', word['x'])
			if re.search(r'[aeiouy]re$', original.lower()):
				word['ph'] = re.sub(r'[aeiouy]re$', 'AH W', word['ph'])
				word['n'] = re.sub(r'are$', 'aeh', word['n'])
				word['x'] = re.sub(r'are$', 'aeh', word['x'])
				word['n'] = re.sub(r'[eiouy]re$', r'\1ah', word['n'])
				word['x'] = re.sub(r'[eiouy]re$', r'\1ah', word['x'])
			elif re.search(r'[aeiou]r+$', original.lower()):
				word['ph'] = re.sub(r'[aeiou]r+$', 'AH W', word['ph'])
				word['n'] = re.sub(r'er$', 'ah', word['n'])
				word['x'] = re.sub(r'er$', 'ah', word['x'])
			elif re.search(r'r$', original.lower()):
				word['ph'] = re.sub(r'r+$', 'HH', word['ph'])
				word['n'] = re.sub(r'r$', 'h', word['n'])
				word['x'] = re.sub(r'r$', 'h', word['x'])
			elif re.search(r's$', original.lower()):
				word['ph'] = re.sub(r's$', 'S', word['ph'])
				word['n'] = re.sub(r's$', '-s', word['n'])
				word['x'] = re.sub(r's$', '-s', word['x'])
			
			if re.search(r'ng', original.lower()):
				word['ph'] = re.sub(r'ng', 'NG', word['ph'])
				word['n'] = re.sub(r'ng', 'n-g', word['n'])
				word['x'] = re.sub(r'ng', 'n-g', word['x'])
			if re.search(r'r', original.lower()):
				word['ph'] = re.sub(r'r+h?', 'W', word['ph'])
				word['n'] = re.sub(r'r+h?', 'w', word['n'])
				word['x'] = re.sub(r'r+h?', 'w', word['x'])
			if re.search(r'ch', original.lower()):
				word['ph'] = re.sub(r'ch', 'SH', word['ph'])
				word['m'] = re.sub(r'ch', 'sh', word['n'])
				word['n'] = re.sub(r'ch', 'sh', word['n'])
				word['x'] = re.sub(r'ch', 'sh', word['x'])
			if re.search(r'j', original.lower()):
				word['ph'] = re.sub(r'j', 'ZH', word['ph'])
				word['n'] = re.sub(r'j', 'zh', word['n'])
				word['x'] = re.sub(r'j', 'zh', word['x'])
			if re.search(r'p(?!h)', original.lower()):
				word['ph'] = re.sub(r'p(?!h)', 'B', word['ph'])
				word['n'] = re.sub(r'p(?!h)', 'b', word['n'])
				word['x'] = re.sub(r'p(?!h)', 'b', word['x'])
			if re.search(r't(?!h)', original.lower()):
				word['ph'] = re.sub(r't(?!h)', 'D', word['ph'])
				word['n'] = re.sub(r't(?!h)', 'd', word['n'])
				word['x'] = re.sub(r't(?!h)', 'd', word['x'])
			if re.search(r'(?:ck|kh|k|q)', original.lower()):
				word['ph'] = re.sub(r'(?:ck|kh|k|q)', 'G', word['ph'])
				word['n'] = re.sub(r'(?:ck|kh|k|q)', 'g', word['n'])
				word['x'] = re.sub(r'(?:ck|kh|k|q)', 'g', word['x'])
			if re.search(r'^h', original.lower()):
				word['ph'] = re.sub(r'^h', "'", word['ph'])
				word['n'] = re.sub(r'^h', "'", word['n'])
				word['x'] = re.sub(r'^h', "'", word['m'])
		return word
	
	def split_word(self, word, phonetic, phoneme_library):
		phonemes = re.split(r' ', phonetic)
		letters = []
		if self.log_level >= 6:
			print(word, ":", phonemes)
		
		output = {
			'letters': [],
			'phonemes': []
		}
		re_phoneme = re.compile(r'\d+$')
		# HH AH T
		for i in range(0, len(phonemes)):
			phoneme = re_phoneme.sub(r'', phonemes[i], count=1)
			output['phonemes'].append(phoneme)
			next = ''
			if i < (len(phonemes)-1):
				next = re_phoneme.sub(r'', phonemes[i+1], count=1)
			if self.log_level >= 7:
				print(i, " phoneme:", phoneme, "next:", next)
			
			if phoneme in phoneme_library:
				found = False
				for map in phoneme_library[phoneme]:
					if self.log_level >= 7:
						print("    map:", map)
					ph_match = re.compile(map['phoneme'], re.IGNORECASE)
					ltr_match = ph_match.match(word)
					if ltr_match:
						if self.log_level >= 7:
							print("      match", ltr_match.group(0))
						if 'not_tail' in map and next:
							if self.log_level >= 7:
								print("        check next")
							tail_match = re.compile(map['not_tail'])
							if tail_match.match(next):
								if self.log_level >= 7:
									print("          rejected because of tail!")
								continue
						word = ph_match.sub('', word, count=1)
	# 					letter = map['phoneme']
						letter = ltr_match.group(0)
						# Catch silent e
						if not next and word == 'e':
							letter += "e"
						output['letters'].append(letter)
						found = True
						if self.log_level >= 7:
							print("        Found!")
						break
				if not found:
					output['letters'].append("")
					if self.log_level >= 7:
						print("    Failed to find match in word for", phoneme, "in", word)
			else:
				output['letters'].append("")
				if self.log_level >= 7:
					print("    Failed to find phoneme in library for", phoneme)
		return output
	
