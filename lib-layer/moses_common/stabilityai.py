# print("Loaded StabilityAI module")

import io
import json
import os
import random
import re
import requests

from PIL import Image
from PIL.PngImagePlugin import PngInfo
from stability_sdk import client
import stability_sdk.interfaces.gooseai.generation.generation_pb2 as generation

import moses_common.__init__ as common


"""
import moses_common.stabilityai
"""

class StabilityAI:
	"""
	stabilityai = moses_common.stabilityai.StabilityAI()
	stabilityai = moses_common.stabilityai.StabilityAI(
		stability_key = 'sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
		save_directory = os.environ['HOME'] + '/Downloads',
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, stability_key=None, save_directory=None, model=None, log_level=5, dry_run=False):
		self.log_level = log_level
		self.dry_run = dry_run
		
		os.environ['STABILITY_HOST'] = 'grpc.stability.ai:443'
		self.stability_key = stability_key or os.environ.get('STABILITY_KEY')
		if not self.stability_key:
			raise KeyError("A Stability.ai API key is required from https://dreamstudio.ai/account. It can be passed as an arg or set as STABILITY_KEY env var.")
		
		self.save_directory = save_directory
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def stability_key(self):
		return self._stability_key
	
	@stability_key.setter
	def stability_key(self, value=None):
		if type(value) is str:
			if not re.match(r'sk-[A-Za-z0-9]{48}', value):
				raise ValueError("Invalid API key format")
			self._stability_key = value
	
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
	
	def get_engine_list(self):
		url = "https://api.stability.ai/v1/engines/list".format(os.environ['STABILITY_HOST'])
		print("url {}: {}".format(type(url), url))
		response = requests.get(url, headers={
			"Authorization": f"Bearer {self.stability_key}"
		})
		if response.status_code != 200:
			return None
		
		payload = response.json()
		return payload




class StableDiffusion(StabilityAI):
	"""
	stable_diffusion = moses_common.stabilityai.StableDiffusion()
	stable_diffusion = moses_common.stabilityai.StableDiffusion(
		stability_key = 'sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
		save_directory = os.environ['HOME'] + '/Downloads',
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, stability_key=None, save_directory=None, model='sdxl10', log_level=5, dry_run=False):
		super().__init__(stability_key=stability_key, save_directory=save_directory, log_level=log_level, dry_run=dry_run)
		
		if model == 'sd15':
			engine = 'stable-diffusion-v1-5'
			self._name = 'sd15'
		elif model == 'sdxl':
			engine = 'stable-diffusion-xl-beta-v2-2-2'
			self._name = 'sdxl'
		elif model == 'sdxl09':
			engine = 'stable-diffusion-xl-1024-v0-9'
			self._name = 'sdxl09'
		elif model == 'sdxl10':
			engine = 'stable-diffusion-xl-1024-v1-0'
			self._name = 'sdxl10'
		else:
			raise ValueError("Invalid model")
		
		self._stability_api = client.StabilityInference(
			key = self.stability_key,
				# API Key reference.
			verbose = True,
				# Print debug messages.
			engine = engine
				# Set the engine to use for generation.
				# Available engines: stable-diffusion-v1 stable-diffusion-v1-5 stable-diffusion-512-v2-0 stable-diffusion-768-v2-0
				# stable-diffusion-512-v2-1 stable-diffusion-768-v2-1 stable-diffusion-xl-beta-v2-2-2 stable-inpainting-v1-0 stable-inpainting-512-v2-0
		)
	
	@property
	def name(self):
		return self._name
	
	@property
	def label(self):
		if self._name == 'sd15':
			return "Stable Diffusion 1.5"
		elif self._name == 'sdxl09':
			return "Stable Diffusion XL 0.9"
		elif self._name == 'sdxl10':
			return "Stable Diffusion XL 1.0"
		return "Stable Diffusion XL"
	
	def get_resolution(self, ar=None):
		if not ar:
			ar = 1.0
		ar = common.convert_to_float(ar)
		width = 1024
		height = 1024
		
		if self.name in ['sd15', 'sdxl']:
			if ar >= 1.625:
				width = 896
				height = 512
			elif ar >= 1.415:
				width = 768
				height = 512
			elif ar >= 1.29:
				width = 683
				height = 512
			elif ar >= 1.125:
				width = 640
				height = 512
			
			elif ar >= 0.9:
				width = 512
				height = 512
			
			elif ar >= 0.775:
				width = 512
				height = 640
			elif ar >= 0.71:
				width = 512
				height = 683
			elif ar >= 0.62:
				width = 512
				height = 768
			else:
				width = 640
				height = 896
		
		else:
			if ar >= 2.075:
				width = 1536
				height = 640
			elif ar >= 1.605:
				width = 1344
				height = 768
			elif ar >= 1.375:
				width = 1216
				height = 832
			elif ar >= 1.145:
				width = 1152
				height = 896
			
			elif ar >= 0.89:
				width = 1024
				height = 1024
			
			elif ar >= 0.73:
				width = 896
				height = 1152
			elif ar >= 0.625:
				width = 832
				height = 1216
			elif ar >= 0.495:
				width = 768
				height = 1344
			else:
				width = 640
				height = 1536
		return ar, width, height
	
	"""
	stable_diffusion.text_to_image(prompt)
	stable_diffusion.text_to_image(
		prompt,
		negative_prompt=string,
		filename=filename,
		seed=int,
		steps=int,
		cfg_scale=float,
		orientation='square' || 'landscape' || 'portrait',
		aspect='square' || 'full' || '35' || 'hd'
	)
	
			~ratio	sdxl10		sdxlbeta	sd20
	ultra	21:9	1536x640							2.40	0.42
	ultra	2:1								1024x512	2.00	0.50
			1.85:1							947x512		1.85	0.54
	hd		16:9							910x512		1.78	0.56
	hd		7:4		1344x768	896x512		896x512		1.75	0.57
	35		3:2					768x512		768x512		1.50	0.67
	35		3:2		1216x832							1.46	0.68
			4:3					683x512		683x512		1.33	0.75
	full	5:4		1152x896							1.29	0.78
	full	5:4					640x512		640x512		1.25	0.80
	square	1:1		1024x1024	512x512		512x512		1		1
	
	"""
	def text_to_image(self,
		prompt,
		filename=None,
		negative_prompt=None,
		seed=None,
		steps=None,
		cfg_scale=None,
		filename_prefix=None,
		filename_suffix=None,
		return_args=False,
		
		aspect_ratio=None,
		orientation=None,
		aspect=None
	):
		
		data = prompt
		if type(prompt) is not dict:
			now = common.get_dt_now()
			data = {
				"create_time": now.isoformat(' '),
				"engine_label": self.label,
				"engine_name": self.name,
				"prompt": prompt,
				"filename": filename,
				"seed": int(str(random.randrange(1000000000)).zfill(9)),
				"steps": 30,
				"cfg_scale": 7.0
			}
			
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
			
			# Resolution
			data['aspect_ratio'], data['width'], data['height'] = self.get_resolution(aspect_ratio)
			
			# Filename
			if not data['filename']:
				if not self.save_directory:
					raise ValueError("A filepath or save directory is required.")
				
				qfilename_prefix = ''
				if filename_prefix:
					qfilename_prefix = filename_prefix + '-'
				
				qfilename_suffix = ''
#				if data['orientation'] != 'square':
#					qfilename_suffix = '-' + data['orientation']
#				if data['aspect'] != 'square':
#					qfilename_suffix += '-' + data['aspect']
				
				if filename_suffix:
					qfilename_suffix = '-' + filename_suffix
				
				ts = str(common.get_epoch())
				data['filename'] = '{}{}-{}-{}{}.png'.format(qfilename_prefix, ts, self.name, data['seed'], qfilename_suffix)
				data['filepath'] = '{}/{}'.format(self.save_directory, data['filename'])
		
			if return_args:
				return data
		
		if self.log_level >= 7:
			print(data)
		
		# Generate image
		sd_prompt = [
				generation.Prompt(
					text = data['prompt'],
					parameters = generation.PromptParameters(weight=1)
				)
			]
		if negative_prompt:
			sd_prompt.append(
				generation.Prompt(
					text = data['negative_prompt'],
					parameters = generation.PromptParameters(weight=-1.3)
				)
			)
		
		if self.dry_run:
			return True, data
		
		if self.name == 'sd15':
			print(f"Using {self.label}")
			answers = self._stability_api.generate(
				prompt = sd_prompt,
				seed = data['seed'],
					# If a seed is provided, the resulting generated image will be deterministic.
					# What this means is that as long as all generation parameters remain the same, you can always recall the same image simply by generating it again.
					# Note: This isn't quite the case for CLIP Guided generations, which we tackle in the CLIP Guidance documentation.
				steps = data['steps'],
					# Amount of inference steps performed on image generation. Defaults to 30.
				cfg_scale = data['cfg_scale'],
					# Influences how strongly your generation is guided to match your prompt.
					# Setting this value higher increases the strength in which it tries to match your prompt.
					# Defaults to 7.0 if not specified.
				width = data['width'],
					# Generation width, defaults to 512 if not included.
				height = data['height'],
					# Generation height, defaults to 512 if not included.
				samples = 1,
					# Number of images to generate, defaults to 1 if not included.
#				sampler = generation.SAMPLER_K_DPMPP_2M,
					# Choose which sampler we want to denoise our generation with.
					# Defaults to k_dpmpp_2m if not specified. Clip Guidance only supports ancestral samplers.
					# (Available Samplers: ddim, plms, k_euler, k_euler_ancestral, k_heun, k_dpm_2, k_dpm_2_ancestral, k_dpmpp_2s_ancestral, k_lms, k_dpmpp_2m, k_dpmpp_sde)
				guidance_preset=generation.GUIDANCE_PRESET_FAST_GREEN
					# Enables CLIP Guidance. 
			)
		else:
			print(f"Using {self.label}")
			answers = self._stability_api.generate(
				prompt = sd_prompt,
				seed = data['seed'],
					# If a seed is provided, the resulting generated image will be deterministic.
					# What this means is that as long as all generation parameters remain the same, you can always recall the same image simply by generating it again.
					# Note: This isn't quite the case for CLIP Guided generations, which we tackle in the CLIP Guidance documentation.
				steps = data['steps'],
					# Amount of inference steps performed on image generation. Defaults to 30.
				cfg_scale = data['cfg_scale'],
					# Influences how strongly your generation is guided to match your prompt.
					# Setting this value higher increases the strength in which it tries to match your prompt.
					# Defaults to 7.0 if not specified.
				width = data['width'],
					# Generation width, defaults to 512 if not included.
				height = data['height'],
					# Generation height, defaults to 512 if not included.
				samples = 1,
					# Number of images to generate, defaults to 1 if not included.
				sampler = generation.SAMPLER_K_DPMPP_2M,
					# Choose which sampler we want to denoise our generation with.
					# Defaults to k_dpmpp_2m if not specified. Clip Guidance only supports ancestral samplers.
					# (Available Samplers: ddim, plms, k_euler, k_euler_ancestral, k_heun, k_dpm_2, k_dpm_2_ancestral, k_dpmpp_2s_ancestral, k_lms, k_dpmpp_2m, k_dpmpp_sde)
				guidance_preset=generation.GUIDANCE_PRESET_FAST_GREEN
					# Enables CLIP Guidance. 
			)
	
		# Set up our warning to print to the console if the adult content classifier is tripped.
		# If adult content classifier is not tripped, save generated images.
		print(f"Dimensions: {data['width']} x {data['height']}")
		for resp in answers:
			for artifact in resp.artifacts:
				if artifact.finish_reason == generation.FILTER:
					return False, "Your request activated the API's safety filters and could not be processed. Please modify the prompt and try again."
				if artifact.type == generation.ARTIFACT_IMAGE:
					img = Image.open(io.BytesIO(artifact.binary))
					img.save(data['filepath'], pnginfo=self.get_png_info(data))
					return True, data
		return False, "Failed response from StabilityAI"



class StableImage(StabilityAI):
	"""
	stable_image = moses_common.stabilityai.StableImage()
	stable_image = moses_common.stabilityai.StableImage(
		stability_key = 'sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
		save_directory = os.environ['HOME'] + '/Downloads',
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, stability_key=None, save_directory=None, model='sdxl10', log_level=5, dry_run=False):
		super().__init__(stability_key=stability_key, save_directory=save_directory, log_level=log_level, dry_run=dry_run)
		
		self.name = None
		self.endpoint = 'https://api.stability.ai/v2beta/stable-image/generate/'
		if model == 'sd3':
			self.endpoint += 'sd3'
			self.name = 'sd3'
		elif model == 'sicore':
			self.endpoint += 'core'
			self.name = 'sicore'
		elif model == 'siultra':
			self.endpoint += 'ultra'
			self.name = 'siultra'
		else:
			raise ValueError("Invalid model")
	
	@property
	def label(self):
		if self.name == 'sd3':
			return "Stable Diffusion 3"
		elif self.name == 'sicore':
			return "Stable Image Core"
		elif self.name == 'siultra':
			return "Stable Image Ultra"
		return "Stable Diffusion XL"
	
	def get_resolution(self, ar=None):
		if not ar:
			ar = 1.0
		ar = common.convert_to_float(ar)
		width = 1024
		height = 1024
		
		
		aspect = '1:1'
		if ar >= 2.075:
			width = 1536
			height = 640
			aspect = '21:9'
		elif ar >= 1.605:
			width = 1344
			height = 768
			aspect = '16:9'
		elif ar >= 1.375:
			width = 1216
			height = 832
			aspect = '3:2'
		elif ar >= 1.145:
			width = 1152
			height = 896
			aspect = '5:4'
		
		elif ar >= 0.89:
			width = 1024
			height = 1024
			aspect = '1:1'
		
		elif ar >= 0.73:
			width = 896
			height = 1152
			aspect = '4:5'
		elif ar >= 0.625:
			width = 832
			height = 1216
			aspect = '2:3'
		elif ar >= 0.495:
			width = 768
			height = 1344
			aspect = '9:16'
		else:
			width = 640
			height = 1536
			aspect = '9:21'
		return ar, width, height, aspect
	
	"""
	stable_image.text_to_image(prompt)
	stable_image.text_to_image(
		prompt,
		negative_prompt=string,
		filename=filename,
		seed=int,
		steps=int,
		cfg_scale=float,
		orientation='square' || 'landscape' || 'portrait',
		aspect='square' || 'full' || '35' || 'hd'
	)
	
			~ratio	sdxl10		sdxlbeta	sd20
	ultra	21:9	1536x640							2.40	0.42
	ultra	2:1								1024x512	2.00	0.50
			1.85:1							947x512		1.85	0.54
	hd		16:9							910x512		1.78	0.56
	hd		7:4		1344x768	896x512		896x512		1.75	0.57
	35		3:2					768x512		768x512		1.50	0.67
	35		3:2		1216x832							1.46	0.68
			4:3					683x512		683x512		1.33	0.75
	full	5:4		1152x896							1.29	0.78
	full	5:4					640x512		640x512		1.25	0.80
	square	1:1		1024x1024	512x512		512x512		1		1
	
	"""
	def text_to_image(self,
		prompt,
		filename=None,
		negative_prompt=None,
		seed=None,
		steps=None,
		cfg_scale=None,
		filename_prefix=None,
		filename_suffix=None,
		return_args=False,
		
		aspect_ratio=None,
		orientation=None,
		aspect=None
	):
		
		data = prompt
		aspect = '1:1'
		if type(prompt) is not dict:
			now = common.get_dt_now()
			data = {
				"create_time": now.isoformat(' '),
				"engine_label": self.label,
				"engine_name": self.name,
				"prompt": prompt,
				"filename": filename,
				"seed": int(str(random.randrange(1000000000)).zfill(9)),
				"steps": 30,
				"cfg_scale": 7.0
			}
			
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
			
			# Resolution
			data['aspect_ratio'], data['width'], data['height'], aspect = self.get_resolution(aspect_ratio)
			
			# Filename
			if not data['filename']:
				if not self.save_directory:
					raise ValueError("A filepath or save directory is required.")
				
				qfilename_prefix = ''
				if filename_prefix:
					qfilename_prefix = filename_prefix + '-'
				
				qfilename_suffix = ''
#				if data['orientation'] != 'square':
#					qfilename_suffix = '-' + data['orientation']
#				if data['aspect'] != 'square':
#					qfilename_suffix += '-' + data['aspect']
				
				if filename_suffix:
					qfilename_suffix = '-' + filename_suffix
				
				ts = str(common.get_epoch())
				data['filename'] = '{}{}-{}-{}{}.png'.format(qfilename_prefix, ts, self.name, data['seed'], qfilename_suffix)
				data['filepath'] = '{}/{}'.format(self.save_directory, data['filename'])
		
			if return_args:
				return data
		
		if self.log_level >= 7:
			print(data)
		
		# Generate image
		if self.dry_run:
			return True, data
		
		body = {
			"prompt": data['prompt'],
			"negative_prompt": data.get('negative_prompt'),
			"aspect_ratio": aspect,
			"seed": data['seed'],
			"output_format": "png"
		}
		
		print(f"Using {self.label}")
		response = requests.post(
			self.endpoint,
			headers={
				"authorization": f"Bearer {self.stability_key}",
				"accept": "image/*"
			},
			files={"none": ''},
			data=body
		)
		
		if response.status_code == 200:
			with open(data['filepath'], 'wb') as file:
				file.write(response.content)
		else:
			raise Exception(str(response.json()))		
		return True, data



class ESRGAN(StabilityAI):
	"""
	upscaler_2x = moses_common.stabilityai.ESRGAN()
	upscaler_2x = moses_common.stabilityai.ESRGAN(
		stability_key = 'sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, stability_key=None, log_level=5, dry_run=False):
		super().__init__(stability_key=stability_key, log_level=log_level, dry_run=dry_run)
		self._stability_api = client.StabilityInference(
			key = self.stability_key,
				# API Key reference.
			verbose = True,
				# Print debug messages.
			upscale_engine = "esrgan-v1-x2plus"
				# The name of the upscaling model we want to use.
				# Available Upscaling Engines: esrgan-v1-x2plus, stable-diffusion-x4-latent-upscaler 
		)
	
	@property
	def label(self):
		return "Real-ESRGAN"
	
	"""
	upscaler_2x.upscale_image(filepath)
	"""
	def upscale_image(self, source_filepath):
	
		# Import our local image to use as a reference for our upscaled image.
		# The 'img' variable below is set to a local file for upscaling, however if you are already running a generation call and have an image artifact available, you can pass that image artifact to the upscale function instead.
		img = Image.open(source_filepath)
		data = img.info
		if type(data) is not dict:
			data = {}
		
		data['upscale-engine'] = self.label
		
		if img.width > 1024 or img.height > 1024:
			return False, "Source image is too large for 4x upscaler. Must be 1024x1024 or smaller."

		data['upscale-width'] = img.width * 2
		data['upscale-height'] = img.height * 2
		
		if re.search(r'-2x\.png', source_filepath):
			data['upscale-filepath'] = re.sub(r'-2x\.png$', '-4x.png', source_filepath, re.IGNORECASE)
		else:
			data['upscale-filepath'] = re.sub(r'\.png$', '-2x.png', source_filepath, re.IGNORECASE)
		
		if self.log_level >= 7:
			print(data)
		
		if self.dry_run:
			return True, data
		
		answers = self._stability_api.upscale(
			init_image=img,
				# Pass our image to the API and call the upscaling process.
			width = data['upscale-width']
				# width=1024, # Optional parameter to specify the desired output width.
		)
	
		# Set up our warning to print to the console if the adult content classifier is tripped.
		# If adult content classifier is not tripped, save our image.
	
		for resp in answers:
			for artifact in resp.artifacts:
				if artifact.finish_reason == generation.FILTER:
					return False, "Your request activated the API's safety filters and could not be processed. Please modify the prompt and try again."
				if artifact.type == generation.ARTIFACT_IMAGE:
					big_img = Image.open(io.BytesIO(artifact.binary))
					big_img.save(data['upscale-filepath'], pnginfo=self.get_png_info(data))
					return True, data
		return False, "Failed response from StabilityAI"



class LatentUpscaler(StabilityAI):
	"""
	upscaler_4x = moses_common.stabilityai.LatentUpscaler()
	upscaler_4x = moses_common.stabilityai.LatentUpscaler(
		stability_key = 'sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, stability_key=None, log_level=5, dry_run=False):
		super().__init__(stability_key=stability_key, log_level=log_level, dry_run=dry_run)
		self._stability_api = client.StabilityInference(
			key = self.stability_key,
				# API Key reference.
			verbose = True,
				# Print debug messages.
			upscale_engine = "stable-diffusion-x4-latent-upscaler"
				# The name of the upscaling model we want to use.
				# Available Upscaling Engines: esrgan-v1-x2plus, stable-diffusion-x4-latent-upscaler 
		)
	
	@property
	def label(self):
		return "Stable Diffusion Latent Upscaler"
	
	"""
	upscaler_4x.upscale_image(filepath)
	upscaler_4x.upscale_image(
		filepath,
		width
	)
	"""
	def upscale_image(self, source_filepath):
	
		# Import our local image to use as a reference for our upscaled image.
		# The 'img' variable below is set to a local file for upscaling, however if you are already running a generation call and have an image artifact available, you can pass that image artifact to the upscale function instead.
		img = Image.open(source_filepath)
		data = img.info
		if type(data) is not dict:
			data = {}
		
		data['upscale-engine'] = self.label
		
		if img.width > 512 or img.height > 768:
			return False, "Source image is too large for 4x upscaler. Must be 512x768 or smaller."
		
		data['upscale-width'] = img.width * 4
		data['upscale-height'] = img.height * 4
		
		data['upscale-filepath'] = re.sub(r'\.png$', '-4x.png', source_filepath, re.IGNORECASE)
		
		if self.log_level >= 7:
			print(data)
		
		if self.dry_run:
			return True, data
		
		answers = self._stability_api.upscale(
			init_image = img,
				# Pass our image to the API and call the upscaling process.
			width = data['upscale-width'],
				# width=1024, # Optional parameter to specify the desired output width.
			prompt = data.get('prompt'),
				# prompt="A beautiful sunset", # Optional parameter when using `stable-diffusion-x4-latent-upscaler` to specify a prompt to use for the upscaling process.
			seed = int(data.get('seed'))
				# seed=1234, # Optional parameter when using `stable-diffusion-x4-latent-upscaler` to specify a seed to use for the upscaling process.
				# steps=20, # Optional parameter when using `stable-diffusion-x4-latent-upscaler` to specify the number of diffusion steps to use for the upscaling process. Defaults to 20 if no value is passed, with a maximum of 50.
				# cfg_scale=7 # Optional parameter when using `stable-diffusion-x4-latent-upscaler` to specify the strength of prompt in use for the upscaling process. Defaults to 7 if no value is passed.
		)
	
		# Set up our warning to print to the console if the adult content classifier is tripped.
		# If adult content classifier is not tripped, save our image.
	
		for resp in answers:
			for artifact in resp.artifacts:
				if artifact.finish_reason == generation.FILTER:
					return False, "Your request activated the API's safety filters and could not be processed. Please modify the prompt and try again."
				if artifact.type == generation.ARTIFACT_IMAGE:
					big_img = Image.open(io.BytesIO(artifact.binary))
					big_img.save(data['upscale-filepath'], pnginfo=self.get_png_info(data))
					return True, data
		return False, "Failed response from StabilityAI"
