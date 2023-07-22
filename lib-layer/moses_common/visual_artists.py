# print("Loaded Visual Artists module")

import os
import random
import re
import requests
import urllib

import moses_common.__init__ as common
import moses_common.dynamodb
import moses_common.openai
import moses_common.ui

artist_table = moses_common.dynamodb.Table('artintelligence.gallery-artists')
artist_list = None

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
		artist_table.log_level = log_level
		
		self._artist_list_filename = artist_list_location + '/.visual_artists.json'
	
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def artist_count(self):
		return artist_table.item_count
	
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
		# Remove escaped single quotes; they are invalid in JSON
		response_data = re.sub(r"\\'", "'", response_data)
		
		if self._dry_run:
			self._ui.dry_run(f"Write to '{self.artist_list_filename}'")
		else:
			common.write_file(self.artist_list_filename, response_data)
	
	def get_all_artist_ids(self):
		artists_ids = artist_table.get_keys_as_list()
		return artists_ids
	
	def sync_artists_to_db(self):
		old_artists_ids = self.get_all_artist_ids()
		self.retrieve_artist_list()
		new_artists = common.read_file(self.artist_list_filename)
		updated_cnt = 0
		for new_artist in new_artists:
			artist = Artist(
				self,
				new_artist,
				log_level = self.log_level,
				dry_run = self._dry_run
			)
			if artist.id not in old_artists_ids:
				if log_level >= 6:
					self._ui.body(f"  Add artist {artist.name}")
				new_artist['id'] = artist.id
				artist_table.put_item(new_artist)
				updated_cnt += 1
		return updated_cnt
	
	def get_artists(self):
		global artist_list
		
		if not artist_list:
			artist_list = artist_table.scan()
			artist_list = sorted(artist_list, key=lambda artist: artist['Name']) 
		
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
		artists = self.get_artists()
		for artist in artists:
			artist_match = re.compile(r'\b{}\b'.format(common.normalize(artist_name)))
			if re.search(artist_match, common.normalize(artist.name)):
				return artist
		return None
	
	def choose_subject(self):
		centuries, subjects, styles = self.get_categories()
		
		total = 0
		for info in subjects.values():
			total += info['weight']
			info['cumulative'] = total
		score = random.randint(1,total)
		
		subject = None
		for name, info in subjects.items():
			if score <= info['cumulative']:
				return name
		return None
		
	def choose_artist(self, subject=None):
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
					if not subject or subject in artist.categories:
						artist_batch.append(artist)
						break
		
		print("# of artists: " + str(len(artist_batch)))
		index = random.randrange(len(artist_batch))
		return artist_batch[index]
	
	"""
	art_forms = collective.get_art_forms()
	"""
	def get_art_forms(self):
		art_forms = {
			"Illustration": {
				"name": "illustration",
				"methods": ["Aquatint", "Chalk", "Charcoal", "Engraving", "Ink", "Linocut", "Lithography", "Pencil", "Print", "Screen Print", "Watercolor", "Woodcut", "Woodblock Print"]
			},
			"Drawing": {
				"name": "drawing"
			},
			"Painting": {
				"name": "painting",
				"methods": ["Acrylic", "Gouache", "Guache", "Lithography", "Oil", "Pastel", "Pastels", "Tempera", "Watercolor"]
			},
			"Photography": {
				"name": "photograph"
			},
			"Sculpture": {
				"name": "sculpture",
				"methods": ["Marble"]
			}
		}
		art_forms['Drawing']['methods'] = art_forms['Illustration']['methods']
		return art_forms
	
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
		countries = ["Albania", "Argentina", "Armenia", "Australia", "Austria", "Belarus", "Belgium", "Brazil", "Bulgaria", "Canada", "China", "Cocos Islands", "Colombia", "Costa Rica", "Crimea", "Croatia", "Czech Republic", "Denmark", "Egypt", "Estonia", "Finland", "Flemish", "France", "Germany", "Greece", "Guatemala", "Guernsey", "Haiti", "Hungary", "India", "Iraq", "Ireland", "Israel", "Italy", "Japan", "Latvia", "Lithuania", "Mexico", "Netherlands", "New Zealand", "Nigeria", "Norway", "Palestine", "Peru", "Philippines", "Poland", "Portugal", "Puerto Rico", "Romania", "Russia", "Scotland", "Serbia", "Singapore", "Slovakia", "South Africa", "South Korea", "Spain", "Sweden", "Switzerland", "Taiwan", "Turkey", "UK", "Ukraine", "Uruguay", "USA", "Vietnam"]
		# "A painting/illustration of {subject}"
		subjects = {
			"Anatomy":			{ "weight": 2,		"prompt": "anatomy" },
			"Architecture":		{ "weight": 3,		"prompt": "architecture" },
			"Botanical":		{ "weight": 19,		"prompt": "botanical" },
			"Cat":				{ "weight": 2,		"prompt": "cats" },
			"Character Design":	{ "weight": 0,		"prompt": "a character design" },
			"Cityscape":		{ "weight": 17,		"prompt": "a cityscape" },
			"Dance":			{ "weight": 0,		"prompt": "dance" },
			"Landscape":		{ "weight": 132,	"prompt": "a landscape" },
			"Logo":				{ "weight": 0,		"prompt": "a logo" },
			"Marine":			{ "weight": 13,		"prompt": "a marine seascape" },
			"Nudity":			{ "weight": 0,		"prompt": "nudity" },
			"Ornithology":		{ "weight": 2,		"prompt": "ornithology" },
			"Portrait":			{ "weight": 0,		"prompt": "a portrait" },
			"Still Life":		{ "weight": 2,		"prompt": "a still life" }
		}
		# " in the style of {style}"
		styles = {
			"3D": "in a 3D style",
			"abstract": "in an abstract style",
			"animation": "in an animation style",
			"anime": "in the style of anime",
			"Art Deco": "in style of Art Deco",
			"Art Nouveau": "in the style of Art Nouveau",
			"avant-garde": "in the style of avant-garde",
			"author": "in the style of an author",
			"b&w": "in b&w",
			"Baroque": "in the style of Baroque",
			"Bauhaus": "in the style of Bauhaus",
			"Blizzard": "in the style of Blizzard",
			"cartoon": "in the style of a cartoon",
			"children's book": "in the style of a children's book",
			"collage": "as a collage",
			"comic": "in the style of a comic",
			"concept art": "in the style of concept art",
			"cover art": "in the style of cover art",
			"covert art": "in the style of cover art",
			"Cubism": "in the style of Cubism",
			"Dada": "in the style of Dada",
			"DC Comics": "in the style of DC Comics",
			"Disney": "in the style of Disney",
			"DnD": "in the style of DnD",
			"DragonBall": "in the style of DragonBall",
			"Dune": "in the style of Dune",
			"Edwardian": "in an Edwardian style",
			"Expressionism": "in the style of Expressionism",
			"fantasy": "in the style of fantasy",
			"fashion": "in the style of fashion",
			"fashion designer": "in the style of a fashion designer",
			"flat style": "in flat style",
			"Futurism": "in the style of Futurism",
			"game art": "in the style of game art",
			"Ghibli": "in the style of Ghibli",
			"Gothic": "in the style of Gothic",
			"grafitti": "in the style of grafitti",
			"graffiti": "in the style of grafitti",
			"graphic design": "in the style of graphic design",
			"graphic novel": "in the style of a graphic novel",
			"Hearthstone": "in the style of Hearthstone",
			"horror": "in the style of horror",
			"illustrator": "in the style of an illustrator",
			"Impressionism": "in the style of Impressionism",
			"industrial design": "in the style of industrial design",
			"interior": "in the style of interior design",
			"Jim Henson": "in the style of Jim Henson",
			"Konami": "in the style of Konami",
			"light installation": "in the style of a light installation",
			"logo": "in the style of a logo",
			"Mad Magazine": "in the style of Mad Magazine",
			"manga": "in the style of manga",
			"Marvel": "in the style of Marvel",
			"Minimalism": "in the style of Minimalism",
			"movie director": "in the style of a movie director",
			"morbid": "in a morbid style",
			"MTG": "in the style of MTG",
			"muralismo": "in the style of muralismo",
			"mythology": "in the style of mythology",
			"Neoclassicism": "in the style of Neoclassicism",
			"naive art": "in the style of naive art",
			"occultism": "in the style of occultism",
			"Orientalism": "in the style of Orientalism",
			"pattern": "in the style of a pattern",
			"pin-ups": "in the style of a pin-up",
			"Pointillism": "in the style of Pointillism",
			"Pokemon": "in the style of Pokemon",
			"Pop-Art": "in the style of Pop Art",
			"poster": "in the style of a poster",
			"Postmodernism": "in the style of a Postmodernism",
			"Printer": "in the style of a printer",
			"psychedelic art": "in the style of psychedelic art",
			"rainbow": "in rainbow style",
			"Realism": "in the style of Realism",
			"Rococo": "in the style of Rococo",
			"Romanticism": "in the style of Romanticism",
			"sci-fi": "in the style of sci-fi",
			"silhouette": "in the style of a silhouette",
			"stained glass": "in the style of stained glass",
			"Star Wars": "in the style of Star Wars",
			"street art": "in the style of street art",
			"superflat": "in a superflat style",
			"Surrealism": "in the style of Surrealism",
			"symbolism": "in the style of symbolism",
			"Tolkien": "in the style of Tolkien",
			"TV": "in the style of a TV show",
			"Victorian": "in a Victorian style",
			"visual arts": "in the style of visual arts",
			"wallpaper": "in the style of wallpaper",
			"Warhammer": "in the style of Warhammer",
			"Watchmen": "in the style of Watchmen",
			"Winnie-the-Pooh": "in the style of Winnie-the-Pooh"
		}
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
		self._id = common.convert_to_snakecase(common.normalize(self.name, strip_single_chars=False))
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def id(self):
		return self._id
	
	@property
	def name(self):
		if 'Prompt' in self._data:
			artist_name = re.sub(r'style of ', '', self._data['Prompt'], re.IGNORECASE)
			artist_name = re.sub(r',.*$', '', artist_name)
			artist_name = re.sub(r' *\(.*?\)', '', artist_name)
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
		lower_cats = [element.lower() for element in categories]
		short_list = []
		for item in full_list:
			if item.lower() in lower_cats:
				short_list.append(item)
		if len(short_list):
			index = random.randrange(len(short_list))
			return short_list[index]
		else:
			return None
	
	def get_query(self, chosen_subject=None):
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
		subject=None
		if chosen_subject:
			subject = chosen_subject
		else:
			subject = self.choose_category(list(subjects.keys()), self.categories)
		if subject:
			subject_info = subjects[subject]
			query['subject'] = subject_info['prompt'].lower()
			if subject == 'Botanical' and qart_form:
				qart_form = re.sub(r' of an?', ' of a botanical', qart_form)
			else:
				qsubject = f" of {subject_info['prompt']}"
		
		# Style
		qstyle = ''
		style = self.choose_category(list(styles.keys()), self.categories)
		if style:
			query['style'] = style
			qstyle = f" {styles[style]}"
		
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
		# Whole word portrait
		if prompt and re.search(r'\b(portrait)\b', prompt, re.IGNORECASE):
			orientation = 'portrait'
		# Whole word landscape
		elif prompt and re.search(r'\b(city|coastline|countryside|meadow|seaside|skyline|street scene)\b', prompt, re.IGNORECASE):
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

