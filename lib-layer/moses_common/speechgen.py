# print("Loaded SpeechGen module")

import json
import os
import re
import subprocess

import moses_common.__init__ as common
import moses_common.language.__init__ as lang
import moses_common.language.accent


"""
import moses_common.speechgen
"""

class SpeechGen:
	"""
	speechgen = moses_common.speechgen.SpeechGen()
	speechgen = moses_common.speechgen.SpeechGen(
		speechgen_email = 'user@example.com',
		speechgen_api_token = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
		save_directory = os.environ['HOME'] + '/Downloads',
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, speechgen_email=None, speechgen_api_token=None, save_directory=None, model=None, ui=None, log_level=5, dry_run=False):
		self.log_level = log_level
		self.dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
		
		self.endpoint = 'https://speechgen.io/index.php'
		self.speechgen_api_token = speechgen_api_token or os.environ.get('SPEECHGEN_API_TOKEN')
		if not self.speechgen_api_token:
			raise KeyError("A SpeechGen.io API key is required from https://speechgen.io/en/profile/. It can be passed as an arg or set as SPEECHGEN_API_TOKEN env var.")
		self.speechgen_email = speechgen_email or os.environ.get('SPEECHGEN_EMAIL')
		if not self.speechgen_email:
			raise KeyError("A SpeechGen.io email is required from https://speechgen.io/en/profile/. It can be passed as an arg or set as SPEECHGEN_EMAIL env var.")
		
		self._save_directory = None
		self.save_directory = save_directory
		self._voices = None
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def speechgen_api_token(self):
		return self._speechgen_api_token
	
	@speechgen_api_token.setter
	def speechgen_api_token(self, value=None):
		if type(value) is str:
			if not re.match(r'[A-Fa-f0-9]{32}', value):
				raise ValueError("Invalid API key format")
			self._speechgen_api_token = value
	
	@property
	def save_directory(self):
		return self._save_directory
	
	@save_directory.setter
	def save_directory(self, value=None):
		if type(value) is str:
			value = os.path.expanduser(value)
			if os.path.isdir(value):
				self._save_directory = value
	
	@property
	def voices(self):
		file_path = common.get_storage_dir('speechgen') + "/voices.json"
		
		# Read from cache file
		if not self._voices:
			self._voices = common.read_cache(file_path, 30)
		
		# Read from API
		if not self._voices:
			url = f"{self.endpoint}?r=api/voices"
			response_code, response_data = common.get_url(url)
			if response_code != 200:
				return None
			self._voices = response_data
			success = common.write_file(file_path, response_data, format='json', make_dir=True)
		return self._voices
	
	def get_voice_list(self, code, gender=None):
		language = lang.get_language(code)
		if not language or not language.get('speechgen'):
			return []
		sg_language = self.voices.get(language['speechgen'])
		if not sg_language:
			return []
		voice_list = []
		for voice in sg_language:
			if not gender or gender == voice['sex']:
				voice_list.append(voice['voice'])
		return voice_list


class TTS(SpeechGen):
	"""
	tts = moses_common.speechgen.TTS()
	tts = moses_common.speechgen.TTS(
		speechgen_email = 'user@example.com',
		speechgen_api_token = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
		save_directory = os.environ['HOME'] + '/Downloads',
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, speechgen_email=None, speechgen_api_token=None, save_directory=None, ui=None, log_level=5, dry_run=False):
		super().__init__(speechgen_email=speechgen_email, speechgen_api_token=speechgen_api_token, save_directory=save_directory, log_level=log_level, dry_run=dry_run)
	
	
	def convert_to_phonetics(self, code, text):
		accent = moses_common.language.accent.Accent(code, {}, ui=self.ui, log_level=self.log_level, dry_run=self.dry_run)
		if not accent.dictionary:
			print(f"ERROR: Dictionary not found")
			return None
		return accent.convert_text_to_phonetic(code, text)

	
	
	"""
	response = tts.text_to_speech(text)
	response = tts.text_to_speech(
		text,
		voice="John",
		format="mp3",			# 'mp3', 'wav', 'ogg'
		speed=1.0,				# 0.1 to 2.0
		pitch=0,				# -20 to 20
		emotion="good",			# 'good', 'evil', 'neutral'
		pause_sentence=1000		# in ms
		pause_paragraph=2000	# in ms
		bitrate=48000			# 8000 to 192000 Hz
	)
	Returns
	{
		"id": "32984049",		# unique voice ID
		"status": 1,			# current voiceover status: 0 process, -1 error, 1 success
		"file": "https://speechgen.io/texttomp3/20241116/p_32984031_567.mp3",
		"file_cors": "https://speechgen.io/index.php?r=site/download&prj=32984049&cors=4a9d4eaa56231370dd0f02b396dccdc0",
		"filepath": "/Users/tim/Downloads/john-hello_there_how-2024_11_16-01_44_39.mp3"
		"parts": 0,				# number of voiceovers
		"parts_done": 0,		# number of pieces completed
		"duration": "3",		# audio file duration in seconds, available if status  = 1
		"format": "mp3",		# audio file format
		"error": "",			# error text, in the event of, if status =  -1
		"balans": "66648.5",	# limit balance
		"cost": "0"				# voiceover costs
	}
	"""
	def text_to_speech(self,
		text,
		ref_name=None,
		voice="John",
		format='mp3',
		speed=1.0,
		pitch=0,
		emotion='good',
		pause_sentence=1000,
		pause_paragraph=2000,
		bitrate=48000
	):
		
		data = {
			"token": self.speechgen_api_token,
			"email": self.speechgen_email,
			"text": text,
			"voice": voice,
			
			"format": format,
			"speed": speed,
			"pitch": pitch,
			"emotion": emotion,
			"pause_sentence": pause_sentence,
			"pause_paragraph": pause_paragraph,
			"bitrate": bitrate
		}
		url = f"{self.endpoint}?r=api/text"
		response_code, response_data = common.get_url(url, args={"data": data})
		
		if response_code != 200:
			return False, f"API call failed with code {response_code}"
		if 'status' not in response_data:
			return False, f"API returned an unexpected response: {response_data}"
		if response_data['status'] < 0:
			if response_data.get('error'):
				return False, response_data['error']
			else:
				return False, f"Request failed but no error given"
		elif response_data['status'] == 0:
			return False, f"Request still processing"
		if response_data.get('file'):
			filepath = self.save_directory + '/' + self.get_filename(data)
			if common.download_url(response_data['file'], filepath):
				response_data['filepath'] = filepath
		return True, response_data
	
	
	def get_filename(self, speech_data):
		filename = speech_data.get('ref_name') or speech_data['voice']
		filename = common.normalize(filename, strip_single_chars=False, delimiter='_')
		
		summary = common.truncate(speech_data['text'], 16, type='character', include_ellipsis=False, remove_newlines=True)
		filename += "-" + common.normalize(summary, strip_single_chars=False, delimiter='_')
		
		filename += "-" + common.get_filename_timestamp()
		return filename + f".{speech_data['format']}"
	
	def play(self, speech_results):
		filepath = None
		if type(speech_results) is dict and speech_results.get('filepath'):
			filepath = speech_results['filepath']
		elif type(speech_results) is dict:
			filepath = speech_results
		else:
			return False
		if not os.path.isfile(filepath):
			return False
		
		subprocess.run(f"afplay {filepath}", shell=True)
		return True