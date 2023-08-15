# print("Loaded OpenAI module")

import os
import re
import requests

from PIL import Image
from PIL.PngImagePlugin import PngInfo
import openai

import moses_common.__init__ as common



"""
import moses_common.openai
"""

class OpenAI:
	"""
	openai = moses_common.openai.OpenAI()
	openai = moses_common.openai.OpenAI(
		openai_api_key = 'sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
		save_directory = os.environ['HOME'] + '/Downloads',
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, openai_api_key=None, save_directory=None, log_level=5, dry_run=False):
		self.log_level = log_level
		self._dry_run = dry_run
		
		self.openai_api_key = openai_api_key or os.environ.get('OPENAI_API_KEY')
		if not self.openai_api_key:
			raise KeyError("An OpenAI API key is required from https://platform.openai.com/account. It can be passed as an arg or set as OPENAI_API_KEY env var.")
		openai.api_key = self.openai_api_key
		
		self.save_directory = save_directory
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def openai_api_key(self):
		return self._openai_api_key
	
	@openai_api_key.setter
	def openai_api_key(self, value=None):
		if type(value) is str:
			if not re.match(r'sk-[A-Za-z0-9]{48}', value):
				raise ValueError("Invalid API key format")
			self._openai_api_key = value
	
	@property
	def save_directory(self):
		return self._save_directory
	
	@save_directory.setter
	def save_directory(self, value=None):
		if type(value) is str and os.path.isdir(value):
			self._save_directory = value

	def get_png_info(self, data):
		png_info = PngInfo()
		flat_data = common.flatten_hash(data)
		for key, value in flat_data.items():
			png_info.add_text(key, str(value))
		return png_info
	


class GPT(OpenAI):
	"""
	gpt = moses_common.openai.GPT()
	gpt = moses_common.openai.GPT(
		openai_api_key = 'sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, openai_api_key=None, model=None, log_level=5, dry_run=False):
		super().__init__(openai_api_key=openai_api_key, log_level=log_level, dry_run=dry_run)
		self.model = model
		
	
	@property
	def model(self):
		return self._model
	
	@model.setter
	def model(self, value):
		if re.match(r'gpt-3', value):
			self._model = 'gpt-3.5-turbo'
		else:
			self._model = 'gpt-4'
	
	@property
	def label(self):
		if re.match(r'gpt-3', self.model):
			return "GPT 3.5"
		else:
			return "GPT 4"
	
	"""
	gpt.chat(prompt)
	"""
	def chat(self, prompt):
		if self.log_level >= 7:
			print(f'openai prompt text: "{prompt}"')
		
		if self._dry_run:
			return "Dry run prompt"
		
		completion = openai.ChatCompletion.create(
			model = self.model,
			messages = [{
				"role": "user",
				"content": prompt
			}],
			temperature = 1.0
		)
		
		answer = None
		if 'choices' not in completion or type(completion['choices']) is not list or not len(completion['choices']):
			if self.log_level >= 6:
				print(completion)
			return None
		else:
			choices = []
			for choice in completion['choices']:
				if 'message' in choice and 'content' in choice['message']:
					answer = choice['message']['content'].rstrip().lstrip()
				elif 'text' in choice:
					answer = choices.append(choice['text'].rstrip().lstrip())
		if not answer:
			if self.log_level >= 6:
				print(completion)
			return None
		if self.log_level >= 7:
			print(completion)
		answer = re.sub(r'\n+', ' ', answer)
		answer = re.sub(r'"', '', answer)
		return answer


class DALLE(OpenAI):
	"""
	dalle = moses_common.openai.DALLE()
	dalle = moses_common.openai.DALLE(
		openai_api_key = 'sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
		save_directory = os.environ['HOME'] + '/Downloads',
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, openai_api_key=None, save_directory=None, log_level=5, dry_run=False):
		super().__init__(openai_api_key=openai_api_key, save_directory=save_directory, log_level=log_level, dry_run=dry_run)
	
	@property
	def name(self):
		return "dalle"
	
	@property
	def label(self):
		return "DALL·E"
	
	"""
	dalle.text_to_image(prompt)
	dalle.text_to_image(prompt,
		filename=path,
		width=512,
		height=512
	)
	"""
	def text_to_image(self,
		prompt,
		filename=None,
		width=None,
		height=None,
		filename_prefix=None,
		filename_suffix=None,
		return_args=False
	):
	
		data = prompt
		if type(prompt) is not dict:
			data = {
				"engine_label": self.label,
				"engine_name": self.name,
				"prompt": prompt,
				"filename": filename,
				"width": 512,
				"height": 512
			}
		
			# Size
			if width:
				data['width'] = common.convert_to_int(width)
			if height:
				data['height'] = common.convert_to_int(height)
		
			# Filename
			if not data['filename']:
				if not self.save_directory:
					raise ValueError("A save directory is required.")
				
				qfilename_prefix = ''
				if filename_prefix:
					qfilename_prefix = filename_prefix + '-'
				
				qfilename_suffix = ''
				if data['width'] > 512:
					qfilename_suffix = '-hires'
				if filename_suffix:
					qfilename_suffix = '-' + filename_suffix
				
				ts = str(common.get_epoch())
				data['filename'] = '{}{}-dalle{}.png'.format(qfilename_prefix, ts, qfilename_suffix)
				data['filepath'] = '{}/{}'.format(self.save_directory, data['filename'])
		
			if return_args:
				return data
		
		if self.log_level >= 7:
			print(data)
		
		if self._dry_run:
			return True, data
		
		# Generate image
		image_resp = openai.Image.create(
			prompt = data['prompt'],
			n = 1,
			size=f"{data['width']}x{data['height']}"
		)
		if self.log_level >= 7:
			print(image_resp)
		
		# Get the image URL from the response
		if not image_resp or 'data' not in image_resp:
			return False, "Failed response from OpenAI"
		source_image_url = image_resp["data"][0]["url"]
		
		if self.log_level >= 7:
			print(f"Image URL: {source_image_url}")
		
		common.download_url(source_image_url, data['filepath'])
		
		# Add metadata
		img = Image.open(data['filepath'])
		img.save(data['filepath'], pnginfo=self.get_png_info(data))
		
		return True, data
	