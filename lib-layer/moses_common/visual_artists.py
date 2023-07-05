# print("Loaded Visual Artists module")

import os
import random
import re
import requests
import urllib

import moses_common.__init__ as common
import moses_common.openai
import moses_common.ui


"""
import moses_common.visual_artists as visual_artists
"""

class Collective:
	"""
	collective = visual_artists.Collective(artist_list_location)
	collective = visual_artists.Collective(artist_list_location, log_level=log_level, dry_run=dry_run)
	collective = visual_artists.Collective(
		artist_list_location,
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, artist_list_location, log_level=5, dry_run=False):
		self.log_level = log_level
		self._dry_run = dry_run
		self._ui = moses_common.ui.Interface()
		
		self._artist_list_filename = artist_list_location + '/.visual_artists.json'
	
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def artist_list_url(self):
		return 'https://supagruen.github.io/StableDiffusion-CheatSheet/src/data.js'
	
	@property
	def artist_list_filename(self):
		return self._artist_list_filename
	
	def has_artist_list(self):
		if os.path.isfile(self.artist_list_filename):
			return True
		return False
	
	def retrieve_artist_list(self):
		self._ui.body("Retrieving artist list...")
		response_code, response_data = common.get_url(self.artist_list_url, dry_run=self._dry_run)
		response_data = re.sub(r'^.*?\[', '[', response_data)
		response_data = re.sub(r';$', '', response_data)
		
		if self._dry_run:
			self._ui.dry_run(f"Write to '{self.artist_list_filename}'")
		else:
			common.write_file(self.artist_list_filename, response_data)
	
	def get_artist_list(self):
		if not os.path.isfile(self.artist_list_filename):
			self.retrieve_artist_list()
		return common.read_file(self.artist_list_filename)
	
	def get_artists(self):
		artist_list = self.get_artist_list()
		artists = []
		for artist_data in artist_list:
			artist = Artist(
				self,
				artist_data,
				log_level = self.log_level,
				dry_run = self._dry_run
			)
			artists.append(artist)
		return artists
	
	def get_artist(self, artist_name):
		if re.search(r',', artist_name):
			parts = artist_name.split(',')
			artist_name = parts[1] + ' ' + parts[0]
			print("artist_name {}: {}".format(type(artist_name), artist_name))
		artist_list = self.get_artists()
		for artist in artist_list:
			artist_match = re.compile(r'\b{}\b'.format(common.normalize(artist_name)))
			if re.search(artist_match, common.normalize(artist.name)):
				return artist
		return None
	
	def choose_artist(self):
		artists = self.get_artists()
		art_forms = self.get_art_forms()
		
		artist_batch = []
		negative_categories = self.get_negative_categories()
		for artist in artists:
			# Weed out the weird ones
			skip = False
			for negative in negative_categories:
				if negative in artist.categories:
					skip = True
			if skip:
				continue
			
			# Keep the ones that match
			for main_category in art_forms:
				if main_category in artist.categories:
					artist_batch.append(artist)
					break
	
		index = random.randrange(len(artist_batch))
		return artist_batch[index]
	
	"""
	art_forms = collective.get_art_forms()
	"""
	def get_art_forms(self):
		return {
			"Illustration": {
				"name": "illustration",
				"methods": ["Charcoal", "Engraving", "Ink", "Lithography", "Pencil", "Print", "Screen Print", "Watercolor"]
			},
			"Painting": {
				"name": "painting",
				"methods": ["Acrylic", "Gouache", "Guache", "Lithography", "Oil", "Pastel", "Tempera", "Watercolor"]
			},
			"Photography": {
				"name": "photograph"
			},
			"Sculpture": {
				"name": "sculpture"
			}
		}
	
	"""
	centuries, subjects, styles = collective.get_categories()
	"""
	def get_categories(self):
		centuries = [
			"13th Century",
			"14th Century",
			"15th Century",
			"16th Century",
			"17th Century",
			"18th Century",
			"19th Century"
		]
		subjects = {
			"Architecture": "architecture",
			"Botanical": "botanical",
			"Cityscape": "a cityscape",
			"Landscape": "a landscape",
			"Logo": "a logo",
			"Marine": "a marine seascape",
			"Ornithology": "ornithology",
			"Portrait": "a portrait",
			"Still Life": "a still life"
		}
		styles = [
			"Art Deco",
			"Art Nouveau",
			"Author",
			"Blizzard",
			"Cartoon",
			"Collage",
			"Comic",
			"Concept Art",
			"Cover Art",
			"Disney",
			"DnD",
			"Expressionism",
			"Fantasy",
			"Flat Style",
			"Futurism",
			"Ghibli",
			"Gothic",
			"Grafitti",
			"Hearthstone",
			"Horror",
			"Illustrator",
			"Impressionism",
			"Industrial Design",
			"Jim Henson",
			"Konami",
			"Mad Magazine",
			"Manga",
			"Marvel",
			"MTG",
			"Muralismo",
			"Mythology",
			"Naive Art",
			"Orientalism",
			"Pattern",
			"Pin-Ups",
			"Pointillism",
			"Poster",
			"Psychedelic Art",
			"Rainbow",
			"Realism",
			"Sc-iFi",
			"Sci-Fi",
			"Stained Glass",
			"Star Wars",
			"Street Art",
			"Superflat",
			"Surrealism",
			"Symbolism",
			"Tolkien",
			"Visual Arts",
			"Wallpaper",
			"Warhammer",
			"Winnie-the-Pooh"
		]
		return centuries, subjects, styles
	
	def get_negative_categories(self):
		return [
			'Abstract',
			'Comic',
			'Cubism',
			'Fashion',
			'Game Art',
			'Graphic Novel',
			'Logo',
			'Manga',
			'Pop-Art',
			'Sculpture'
		]



class Artist:
	"""
	artist = visual_artists.Artist(collective, data)
	artist = visual_artists.Artist(collective, {
			"Type": "1",
			"Name": "Abbey, Edwin Austin",
			"Born": "1852",
			"Death": "1911",
			"Prompt": "style of Edwin Austin Abbey",
			"NPrompt": "",
			"Category": "Illustration, Painting, Oil, Pastel, Ink, USA, 19th Century",
			"Checkpoint": "Deliberate 2.0",
			"Extrainfo": "",
			"Image": "Edwin-Austin-Abbey.webp",
			"Creation": "202306200852"
		},
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, collective, data, log_level=5, dry_run=False):
		self.log_level = log_level
		self._dry_run = dry_run
		self._collective = collective
		self._data = data
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def name(self):
		if 'Prompt' in self._data:
			artist_name = re.sub(r'style of ', '', self._data['Prompt'], re.IGNORECASE)
			return artist_name
		if 'Name' in self._data:
			artist_name = re.sub(r' ?\(.*?\)', '', self._data['Name'], re.IGNORECASE)
			if re.search(r',', artist_name):
				name_list = self._data['Name'].split(', ')
				return name_list[1] + ' ' + name_list[0]
			return artist_name
		return None
	
	@property
	def categories(self):
		if 'Category' in self._data:
			categories = self._data['Category'].split(', ')
			return categories
		return []
	
	@property
	def checkpoint(self):
		return self._data.get('Checkpoint')
	
	@property
	def image_url(self):
		if 'Image' in self._data:
			return 'https://supagruen.github.io/StableDiffusion-CheatSheet/img/' + self._data['Image']
		return None
	
	def get_settings(self):
		settings = {}
		if 'Extrainfo' not in self._data:
			return None
		
		extra = self._data['Extrainfo']
		parts = re.search(r'(\d+) steps\b', extra, re.IGNORECASE)
		if parts:
			settings['steps'] = common.convert_to_int(parts.group(1))
		parts = re.search(r'\bsteps:? (\d+)', extra, re.IGNORECASE)
		if parts:
			settings['steps'] = common.convert_to_int(parts.group(1))
		parts = re.search(r'\bcfg scale:? ([0-9.]+)', extra, re.IGNORECASE)
		if parts:
			settings['cfg_scale'] = common.convert_to_float(parts.group(1))
		
		return settings
	
	def choose_category(self, full_list, categories):
		short_list = []
		for item in full_list:
			if item in categories:
				short_list.append(item)
		if len(short_list):
			index = random.randrange(len(short_list))
			return short_list[index]
		else:
			return None
	
	def get_query(self):
		art_forms = self._collective.get_art_forms()
		centuries, subjects, styles = self._collective.get_categories()	
		
		# Assemble query
		query = {
			"artist": self.name,
			"model": self.checkpoint
		}
		
		# Art form and method
		art_forms = self._collective.get_art_forms()
		qart_form = ' of art'
		art_form = self.choose_category(list(art_forms.keys()), self.categories)
		if art_form:
			query['art_form'] = art_forms[art_form]['name']
			art_form_name = art_forms[art_form]['name']
			qart_form = f" of a {art_form_name}"
			if re.match(r'[aeiou]', art_form_name):
				qart_form = f" of an {art_form_name}"
			
			# Method
			if 'methods' in art_forms[art_form]:
				method = self.choose_category(art_forms[art_form]['methods'], self.categories)
				if method:
					query['method'] = method.lower()
					qart_form = f" of a {query['method']} {art_form_name}"
					if re.match(r'[aeiou]', query['method']):
						qart_form = f" of an {query['method']} {art_form_name}"
		
		# Artist
		qartist = f" by {query['artist']}"
		
		# Subject
		qsubject = ''
		subject = self.choose_category(list(subjects.keys()), self.categories)
		if subject:
			query['subject'] = subject.lower()
			if subject == 'Botanical' and qart_form:
				qart_form = re.sub(r' of an?', ' of a botanical', qart_form)
			else:
				qsubject = f" of {subjects[subject]}"
		
		# Style
		qstyle = ''
		style = self.choose_category(styles, self.categories)
		if style:
			query['style'] = style
			qstyle = f" in the style of {query['style']}"
		
		# Century
		qcentury = ''
		century = self.choose_category(centuries, self.categories)
		if century:
			query['century'] = century.lower()
			qcentury = f" from the {query['century']}"
		
		# Form query
		query['query'] = f"Generate a short description{qart_form}{qartist}{qsubject}{qstyle}{qcentury} including a description of the subject and style."
		
		if self.log_level >= 6:
			print("Query: {}".format(common.make_json(query, pretty_print=True)))
		elif self.log_level >= 5:
			print(f"Artist: {query['artist']}")
			print(f"Categories: {self.categories}")
			print(f"Query: {query['query']}")
	
		return query



class Prompt:
	"""
	prompt = visual_artists.Prompt(query)
	prompt = visual_artists.Prompt(query, log_level=log_level, dry_run=dry_run)
	prompt = visual_artists.Prompt(query,
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, query_or_prompt, log_level=5, dry_run=False):
		self.log_level = log_level
		self._dry_run = dry_run
		
		if type(query_or_prompt) is str:
			self._data = {
				"prompt": query_or_prompt
			}
		elif type(query_or_prompt) is dict:
			self._data = {
				"query": query_or_prompt
			}
		else:
			raise TypeError("Invalid query type")
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def prompt(self):
		return self._data.get('prompt')
	
	@property
	def data(self):
		return self._data
	
	def generate(self, openai_api_key=None):
		if 'prompt' not in self._data:
			gpt = moses_common.openai.GPT(openai_api_key=openai_api_key, log_level=self.log_level, dry_run=self._dry_run)
			self._data['prompt'] = gpt.chat(self._data['query']['query'])
		
		self._add_resolution()
		self._improve_prompt()
		
		if self.log_level >= 6:
			print(f"Prompt: {self.data}")
		return self._data
	
	def _add_resolution(self):
		if 'prompt' not in self._data:
			return False
		
		prompt = self._data['prompt']
		orientation = 'square'
		aspect = 'square'
		# Whole word landscape
		if prompt and re.search(r'\b(city|coastline|countryside|meadow|seaside|skyline|street scene)\b', prompt, re.IGNORECASE):
			orientation = 'landscape'
		# *scape landscape
		elif prompt and re.search(r'\b(city|cloud|land|moon|river|sea|sky|snow|town|tree|water)scapes?\b', prompt, re.IGNORECASE):
			orientation = 'landscape'
		# *polis landscape
		elif prompt and re.search(r'\b(acro|cosmo|megalo|metro|necro)polis\b', prompt, re.IGNORECASE):
			orientation = 'landscape'
		# Whole word full
		elif prompt and re.search(r'\b(collage|still life)\b', prompt, re.IGNORECASE):
			orientation = 'landscape'
			aspect = 'full'
		# Whole word portrait
		elif prompt and re.search(r'\b(portrait)\b', prompt, re.IGNORECASE):
			orientation = 'portrait'
	
		width = 768
		height = 768
		if orientation == 'landscape':
			width = 896
			height = 512
			if aspect == 'full':
				width = 640
			elif aspect == '35':
				width = 768
		elif orientation == 'portrait':
			width = 512
			height = 640
			if aspect == '35':
				height = 768
			elif aspect == 'hd':
				height = 896
		
		self._data['width'] = width
		self._data['height'] = height
		self._data['orientation'] = orientation
		self._data['aspect'] = aspect
		return True
	
	def _improve_prompt(self):
		if 'prompt' not in self._data:
			return False
		
		self._data['prompt'] = re.sub(r'^((\w+ ){1,5})on canvas\b', r'\1', self._data['prompt'], re.IGNORECASE)
		self._data['prompt'] = re.sub(r'((\w+ ){0,5})canvas\b', r'\1painting', self._data['prompt'], re.IGNORECASE)
	
		if self._data and 'style' in self._data and self._data['style'] in ['Blizzard', 'Fantasy', 'Hearthstone', 'Mythology', 'Sc-iFi', 'Sci-Fi', 'Star Wars', 'Tolkein', 'Warhammer']:
			self._data['prompt'] += ' Trending on artstation.'
		elif re.search(r'\b(blizzard|fantasy|hearthstone|mythology|sci\-fi|star wars|tolkein|warhammer)\b', self._data['prompt'], re.IGNORECASE):
			self._data['prompt'] += ' Trending on artstation.'
	
	# 	self._data['prompt'] += " Very coherent, 4k, ultra realistic, ultrafine detailed."
		return True
	
	def get_negative_prompt(self, engine=None):
		nps = []
		if engine == 'sinkin':
			nps.append('nude, nsfw')
		if 'orientation' in self._data and self._data['orientation'] != 'square':
			nps.append('duplication artifact')
		
		prompt = self._data.get('prompt')
		if prompt and re.search(r'\b(portrait|person|people|child|children|baby|woman|lady|girl|man|boy)\b', prompt, re.IGNORECASE):
			nps.append('((((ugly)))), (((duplicate))), ((morbid)), ((mutilated)), out of frame, extra fingers, mutated hands, ((poorly drawn hands)), ((poorly drawn face)), (((mutation))), (((deformed))), ((ugly)), blurry, ((bad anatomy)), (((bad proportions))), ((extra limbs)), cloned face, (((disfigured))), out of frame, ugly, extra limbs, (bad anatomy), gross proportions, (malformed limbs), ((missing arms)), ((missing legs)), (((extra arms))), (((extra legs))), mutated hands, (fused fingers), (too many fingers), (((long neck)))')
		if prompt and re.search(r'\b(photo|photograph)\b', prompt, re.IGNORECASE):
			nps.append('illustration, painting, drawing, art, sketch')
		if prompt and re.search(r'\b(illustration|painting)\b', prompt, re.IGNORECASE):
			nps.append('3d, concept art')
		if prompt and re.search(r'\b(canvas|painting)\b', prompt, re.IGNORECASE):
			nps.append('frame, border, wall, hanging, border, canvas')
		nps.append('deformed, disfigured, underexposed, overexposed, lowres, error, cropped, worst quality, low quality, jpeg artifacts, out of frame, watermark, signature')
		return ', '.join(nps)

