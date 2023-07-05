# print("Loaded SinkinAI module")

import os
import random
import re
import requests
import subprocess
import urllib

from PIL import Image
from PIL.PngImagePlugin import PngInfo

import moses_common.__init__ as common



"""
import moses_common.sinkinai
"""

class SinkinAI:
	"""
	sinkinai = moses_common.sinkinai.SinkinAI()
	sinkinai = moses_common.sinkinai.SinkinAI(
		sinkinai_api_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
		save_directory = os.environ['HOME'] + '/Downloads',
		model = 'Deliberate',
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, sinkinai_api_key=None, save_directory=None, log_level=5, dry_run=False):
		self.log_level = log_level
		self._dry_run = dry_run
		
		self._sinkinai_api_key = None
		
		self.sinkinai_api_key = sinkinai_api_key or os.environ.get('SINKINAI_API_KEY')
		if not self.sinkinai_api_key:
			raise KeyError("A sinkin.ai API key is required from https://sinkin.ai/api. It can be passed as an arg or set as SINKINAI_API_KEY env var.")
		
		self.save_directory = save_directory
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def sinkinai_api_key(self):
		return self._sinkinai_api_key
	
	@sinkinai_api_key.setter
	def sinkinai_api_key(self, value=None):
		if type(value) is str:
			if not re.match(r'[a-z0-9]{32}', value):
				raise ValueError("Invalid API key format")
			self._sinkinai_api_key = value
	
	@property
	def save_directory(self):
		return self._save_directory
	
	@save_directory.setter
	def save_directory(self, value=None):
		if type(value) is str and os.path.isdir(value):
			self._save_directory = value
	
	@property
	def name(self):
		return "sinkin"
	
	@property
	def label(self):
		return "sinkin.ai"
	
	@property
	def scheduler(self):
		return "DPMSolverMultistep"
	
	@property
	def endpoint(self):
		return "https://sinkin.ai/m/inference"
	
	def get_png_info(self, data):
		png_info = PngInfo()
		flat_data = common.flatten_hash(data)
		for key, value in flat_data.items():
			png_info.add_text(key, str(value))
		return png_info
	
	"""
	sinkinai.text_to_image(prompt)
	sinkinai.text_to_image(prompt,
		prompt,
		negative_prompt=string,
		model=string,
		filename=name,
		seed=int,
		steps=int,
		cfg_scale=float,
		width=512,
		height=512
	)
	"""
	def text_to_image(self,
		prompt,
		filename=None,
		model=None,
		negative_prompt=None,
		seed=None,
		steps=None,
		cfg_scale=None,
		width=None,
		height=None,
		filename_prefix=None,
		filename_suffix=None,
		return_args=False
	):
	
		data = prompt
		if type(prompt) is not dict:
			now = common.get_dt_now()
			data = {
				"create_time": now.isoformat(' '),
				"engine_label": self.label,
				"engine_name": self.name,
				"model": "Deliberate V2",
				"model_id": "K6KkkKl",
				"model_version": None,
				"prompt": prompt,
				"filename": filename,
				"seed": int(str(random.randrange(1000000000)).zfill(9)),
				"steps": 30,
				"cfg_scale": 7.0,
				"width": 512,
				"height": 512
			}
			
			# Model
			if model:
				if re.match(r'dreamshaper', model, re.IGNORECASE):
					data['model_id'] = "4zdwGOB"
					data['model'] = "DreamShaper"
					data['model_version'] = "5"
			
			# Negative prompt
			if negative_prompt:
				data['negative_prompt'] = str(negative_prompt)
		
			# Seed
			if seed:
				data['seed'] = common.convert_to_int(seed)
		
			# Steps
			if steps:
				data['steps'] = common.convert_to_int(steps)
		
			# CFG scale
			if cfg_scale:
				data['cfg_scale'] = common.convert_to_float(cfg_scale)
		
			# Size
			if width:
				data['width'] = common.convert_to_int(width)
			if height:
				data['height'] = common.convert_to_int(height)
		
			# Filename
			if not data['filename']:
				if not self.save_directory:
					raise ValueError("A filepath or save directory is required.")
				
				qfilename_prefix = ''
				if filename_prefix:
					qfilename_prefix = filename_prefix + '-'
				
				qfilename_suffix = ''
				if data['width'] > data['height']:
					qfilename_suffix = '-landscape'
				elif data['width'] < data['height']:
					qfilename_suffix = '-portrait'
				if data['width'] == 640 or data['height'] == 640:
					qfilename_suffix += '-full'
				elif data['width'] == 768 or data['height'] == 768:
					qfilename_suffix += '-35'
				elif data['width'] == 896 or data['height'] == 896:
					qfilename_suffix += '-hd'
				
				if filename_suffix:
					qfilename_suffix = '-' + filename_suffix
				
				ts = str(common.get_epoch(now))
				data['filename'] = '{}{}-sinkin-{}{}.png'.format(qfilename_prefix, ts, data['seed'], qfilename_suffix)
				data['filepath'] = '{}/{}'.format(self.save_directory, data['filename'])
			
			if return_args:
				return data
		
		if self.log_level >= 7:
			print(data)
		
		# Generate image
		args = {
			"access_token": self.sinkinai_api_key,
			"model_id": data['model_id'],
			"prompt": data['prompt'],
			"width": data['width'],
			"height": data['height'],
			"use_default_neg": "false",
			"steps": data['steps'],
			"scale": data['cfg_scale'],
			"num_images": 1,
			"seed": data['seed'],
			"scheduler": self.scheduler
		}
		if 'model_version' in data and data['model_version']:
			args['version'] = data['model_version']
		if 'negative_prompt' in data and data['negative_prompt']:
			args['negative_prompt'] = data['negative_prompt']
		
		if self._dry_run:
			return True, data
		
		query = urllib.parse.urlencode(args)
		cmd = ["curl", "-s", "-X", "POST", "-H", "Content-Type:multipart/form-data", f"{self.endpoint}?{query}"]
		response = subprocess.check_output(cmd)
		response_data = common.convert_value(response.decode('UTF-8'))
		
		if 'error_code' in response_data and response_data['error_code'] != 0:
			return False, response_data['message']
		
		# Get the image URL from the response
		if 'images' not in response_data or type(response_data['images']) is not list or not len(response_data['images']):
			return False, "No images received from sinkin.ai"
		source_image_url = response_data['images'][0]
		
		if self.log_level >= 7:
			print(f"Image URL: {source_image_url}")
		
		common.download_url(source_image_url, data['filepath'])
		
		# Add metadata
		img = Image.open(data['filepath'])
		img.save(data['filepath'], pnginfo=self.get_png_info(data))
		
		return True, data
