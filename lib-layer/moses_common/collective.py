# print("Loaded Collective module")

import datetime
import os
import random
import re
import requests
import urllib

import moses_common.__init__ as common
import moses_common.dynamodb
import moses_common.google_search
import moses_common.openai
import moses_common.ui

settings_table = moses_common.dynamodb.Table('artintelligence.gallery-settings')
artist_table = moses_common.dynamodb.Table('artintelligence.gallery-collective')
artist_list = None
genres_table = moses_common.dynamodb.Table('artintelligence.gallery-works')
genres_list = None
unique_genre_names = None

"""
import moses_common.collective as collective
"""

class Collective:
	"""
	collective = collective.Collective()
	collective = collective.Collective(log_level=log_level, dry_run=dry_run)
	collective = collective.Collective(
		google_search_api_key = "xxx",
		google_search_project_cx = "xxx",
		openai_api_key = "xxx",
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, google_search_api_key=None, google_search_project_cx=None, openai_api_key=None, log_level=5, dry_run=False):
		self.log_level = log_level
		self.dry_run = dry_run
		self.ui = moses_common.ui.Interface()
		self.artists_last_update = common.get_dt_now()
		self.artists_last_checked = common.get_dt_past(1)
		self.genres_last_update = common.get_dt_now()
		self.genres_last_checked = common.get_dt_past(1)
		self.images_last_update = common.get_dt_now()
		self.images_last_checked = common.get_dt_past(1)
		artist_table.dry_run = dry_run
		artist_table.log_level = log_level
		genres_table.dry_run = dry_run
		genres_table.log_level = log_level
		
		self.gpt = None
		self.openai_api_key = openai_api_key
		self.google_image_search = None
		self.google_search_api_key = google_search_api_key
		self.google_search_project_cx = google_search_project_cx
	
	
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
	def artists(self):
		global artist_list
		
		if not artist_list or self.artists_were_updated():
			artist_records = artist_table.scan()
			artist_records = sorted(artist_records, key=lambda artist: artist['sort_name'])
			
			self.artists_last_update = common.get_dt_now()
			
			artist_list = []
			for artist_data in artist_records:
				artist = Artist(
					self,
					artist_data,
					log_level = self.log_level,
					dry_run = self.dry_run
				)
				artist_list.append(artist)
		return artist_list
	
	@property
	def genres(self):
		global genres_list
		
		if not genres_list or self.genres_were_updated():
			self.ui.warning("Loading genres")
			genres_records = genres_table.scan()
			genres_records = sorted(genres_records, key=lambda genres: genres['name']) 
			
			self.genres_last_update = common.get_dt_now()
			
			genres_list = []
			unique_genre_names = []
			unique_map = {}
			for genres_data in genres_records:
				artist = self.get_artist_by_id(genres_data['artist_id'])
				genre = Genre(
					artist,
					genres_data,
					log_level = self.log_level,
					dry_run = self.dry_run
				)
				genres_list.append(genre)
		
		return genres_list
	
	def get_all_artist_ids(self):
		artists_ids = artist_table.get_keys_as_list()
		return artists_ids
	
	def get_artist_by_id(self, artist_id):
		for artist in self.artists:
			if artist.id == artist_id:
				return artist
		return None
	
	def get_artist_by_name(self, artist_name):
		if not artist_name:
			return None
		for artist in self.artists:
			artist_match = re.compile(r'\b{}\b'.format(common.normalize(artist_name)))
			if re.search(artist_match, common.normalize(artist.name)):
				return artist
		return None
	
	def artists_were_updated(self):
		if self.artists_last_checked + datetime.timedelta(minutes=2) > common.get_dt_now():
# 			print("artists recently checked")
			return False
		self.artists_last_checked = common.get_dt_now()
		print("checking last artists update")
		
		record = settings_table.get_item('artists_last_update')
		dt = common.convert_string_to_datetime(record['value'], tz_aware=True)
		if self.artists_last_update and self.artists_last_update < dt:
			return True
		return False
	
	def set_artists_update(self):
		settings_table.update_item({
			"name": "artists_last_update",
			"value": common.get_dt_now()
		})
		self.artists_last_checked = common.get_dt_past(1)
	
	def genres_were_updated(self):
		if self.genres_last_checked + datetime.timedelta(minutes=2) > common.get_dt_now():
# 			print("genres recently checked")
			return False
		self.genres_last_checked = common.get_dt_now()
		print("checking last genres update")
		
		record = settings_table.get_item('genres_last_update')
		dt = common.convert_string_to_datetime(record['value'], tz_aware=True)
		if self.genres_last_update and self.genres_last_update < dt:
			return True
		return False
	
	def set_genres_update(self):
		settings_table.update_item({
			"name": "genres_last_update",
			"value": common.get_dt_now()
		})
		self.genres_last_checked = common.get_dt_past(1)
	
	def images_were_read(self):
		self.images_last_update = common.get_dt_now()
	
	def images_were_updated(self):
		if self.images_last_checked + datetime.timedelta(minutes=2) > common.get_dt_now():
# 			print("images recently checked")
			return False
		self.images_last_checked = common.get_dt_now()
		print("checking last images update")
		
		record = settings_table.get_item('images_last_update')
		print("record {}: {}".format(type(record), record))
		dt = common.convert_string_to_datetime(record['value'], tz_aware=True)
		if self.images_last_update and self.images_last_update < dt:
			return True
		return False
	
	def set_images_update(self):
		settings_table.update_item({
			"name": "images_last_update",
			"value": common.get_dt_now()
		})
		self.images_last_checked = common.get_dt_past(1)
	
	def choose_category(self, tags):
		if not tags:
			return None
		
		# Disassemble tags
		full_tags = []
		total = 0
		for tag in tags:
			new_tag = {
				"weight": 1,
				"name": tag
			}
			if re.search(r':', tag):
				parts = tag.split(':')
				if not parts[1]:
					parts[1] = None
				new_tag = {
					"weight": common.convert_to_int(parts[0]),
					"name": parts[1]
				}
			total += new_tag['weight']
			new_tag['cumulative'] = total
			full_tags.append(new_tag)
	
		# Choose a tag
		score = random.randint(1,total)
		for tag in full_tags:
			if score <= tag['cumulative']:
				return tag['name']
		return None
	
	def split_tag(self, tag):
		weight = 1
		cat = tag
		if re.search(r':', tag):
			parts = tag.split(':')
			if not parts[1]:
				parts[1] = None
			weight = common.convert_to_int(parts[0])
			cat = parts[1]
		return weight, cat
		
	
	def get_random_work(self, artist_name=None, genre_name=None):
		selected_artist = self.get_artist_by_name(artist_name)
		
		selected_genre = genre_name
		if not selected_genre:
			genres = self.get_genre_list()
			selected_genre = self.choose_category(genres)
		
		batch_list = []
		if selected_artist or selected_genre:
			for genre in self.genres:
				if selected_artist:
					if selected_artist.id == genre.artist_id:
						if genre_name:
							if genre_name == genre.name:
								batch_list.append(genre)
						else:
							batch_list.append(genre)
				elif selected_genre == genre.name:
					batch_list.append(genre)
		else:
			batch_list = self.genres
		
		if not batch_list:
			return None
		index = random.randrange(len(batch_list))
		return batch_list[index]
		
	"""
	genres = collective.get_genre_list()
	"""
	def get_genre_list(self):
		weighted_genre_list = [
			"5:abstract",
			"1:anatomy",
			"5:animal painting",
			"2:animation",
			"5:anime",
			"7:architecture",
			"12:botanical",
			"20:character design",
			"9:cityscape",
			"10:collage",
			"16:comic book",
			"1:cubist",
			"2:dark humor",
			"255:default",
			"39:fantasy",
			"6:fashion",
			"16:genre painting",
			"12:horror",
			"20:illustration",
			"63:landscape",
			"1:manga",
			"2:marine",
			"2:mural",
			"4:photograph",
			"6:pin-up",
			"100:portrait",
			"1:poster",
			"6:psychedelic art",
			"3:religious painting",
			"20:sci-fi",
			"5:sculpture",
			"1:sketch and study",
			"6:still life",
			"8:storybook",
			"42:surreal",
			"1:symbolic painting"
  		]
		
# 			"veduta", # cityscape
# 			"capriccio", # ruins
# 			"tronie",
# 			"pastorale",
# 			"panorama",
			
		unique_genre_names = []
		unique_map = {}
		for genre in self.genres:
			for weighted_genre in weighted_genre_list:
				if weighted_genre != genre.name:
					continue
				if genre.name not in unique_map:
					unique_map[genre.name] = True
					unique_genre_names.append(genre.name)
		return unique_genre_names
	
	"""
	methods = collective.get_all_methods()
	art_methods = [
		"acrylic painting",
		"aquatint print",
		"chalk drawing",
		"charcoal drawing",
		"concept art",
		"digital",
		"engraving",
		"fresco",
		"frieze",
		"glass",
		"gouache painting",
		"ink drawing",
		"linocut",
		"lithograph",
		"marble sculpture",
		"oil painting",
		"painting",
		"pastel",
		"pen drawing",
		"pencil drawing",
		"pencil sketch",
		"photograph",
		"print illustration",
		"screen print",
		"sculpture",
		"tempera painting",
		"watercolor painting",
		"woodblock print",
		"woodcut"
	]
	"""
	def get_all_methods(self):
		method_map = {}
		for genre in self.genres:
			for method in genre.data['methods']:
				weight, tag = self.split_tag(method)
				method_map[tag] = True
		return sorted(method_map.keys())
	
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
			"Architecture":		{ "weight": 6,		"prompt": "architecture" },
			"Beach":			{ "weight": 1,		"prompt": "a beach" },
			"Botanical":		{ "weight": 4,		"prompt": "botanical" },
			"Canal":			{ "weight": 1,		"prompt": "a canal" },
			"Cat":				{ "weight": 2,		"prompt": "cats" },
			"Character Design":	{ "weight": 0,		"prompt": "a character design" },
			"Cityscape":		{ "weight": 17,		"prompt": "a cityscape" },
			"City Street":		{ "weight": 1,		"prompt": "a city street" },
			"Dance":			{ "weight": 0,		"prompt": "dance" },
			"Landscape":		{ "weight": 64,		"prompt": "a landscape" },
			"Logo":				{ "weight": 0,		"prompt": "a logo" },
			"Marine":			{ "weight": 13,		"prompt": "a marine seascape" },
			"Nudity":			{ "weight": 0,		"prompt": "nudity" },
			"Ornithology":		{ "weight": 2,		"prompt": "ornithology" },
			"Portrait":			{ "weight": 5,		"prompt": "a portrait" },
			"Railroad":			{ "weight": 1,		"prompt": "a train" },
			"Still Life":		{ "weight": 2,		"prompt": "a still life" },
			"Windmill":			{ "weight": 1,		"prompt": "a windmill" }
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
	artist = collective.Artist(collective, data)
	artist = collective.Artist(collective, {
			 "id": "floris_arntzenius",
			 "name": "Floris Arntzenius",
			 "sort_name": "Arntzenius, Floris",
			 "born": "1864",
			 "died": "1925",
			 "country": "Netherlands",
			 "external_url": "https://en.wikipedia.org/wiki/Floris_Arntzenius",
			 "bio": "Pieter Florentius Nicolaas Jacobus Arntzenius (9 June 1864 – 16 February 1925) was a Dutch painter...",
			 "create_time": "2023-08-11 11:06:33.182012",
			 "update_time": "2023-09-09 17:46:46.085004"
		},
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, collective, data, log_level=5, dry_run=False):
		self.log_level = log_level
		self.dry_run = dry_run
		self.ui = moses_common.ui.Interface()
		self.collective = collective
		self.data = data
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def id(self):
		return self.data.get('id')
	
	@property
	def name(self):
		return self.data.get('name')
	
	@property
	def sort_name(self):
		return self.data.get('sort_name')
	
	@property
	def external_url(self):
		return self.data.get('external_url')
	
	@external_url.setter
	def external_url(self, value):
		self.data['external_url'] = value.rstrip()
	
	@property
	def bio(self):
		return self.data.get('bio')
	
	@bio.setter
	def bio(self, value):
		self.data['bio'] = value.rstrip()
	
	@property
	def genres(self):
		genres = []
		for genre in self.collective.genres:
			if genre.artist_id != self.id:
				continue
			genres.append(genre)
		return genres
	
	@property
	def genre_data(self):
		genres = []
		for genre in self.collective.genres:
			if genre.artist_id != self.id:
				continue
			genres.append(genre.data)
		return genres
	
	def __str__(self):
		return f"<moses_common.collective.Artist {self.id}>"	
	
	def get_search_results(self, limit=8):
		keywords = 'artwork by ' + self.name
		
		if not self.collective.google_image_search:
			self.collective.google_image_search = moses_common.google_search.ImageSearch(api_key=self.collective.google_search_api_key, project_cx=self.collective.google_search_project_cx, log_level=self.log_level, dry_run=self.dry_run)
		return self.collective.google_image_search.search(keywords, limit=limit)
	
	def save(self):
		success = False
		
		# Update
# 		self.ui.body(f"Update {self.id}")
		dt = common.get_dt_now()
		self.data['update_time'] = common.convert_datetime_to_string(dt)
		success = artist_table.update_item(self.data)
		
		if not success:
			return False
		
		self.collective.set_artists_update()
		return True
				


class Genre:
	"""
	As defined in https://edsitement.neh.gov/lesson-plans/genre-visual-arts-portraits-pears-and-perfect-landscapes
	"Still Life, portrait, and landscape are all categories, or genres, of painting..."
	
	genre = collective.Genre(artist, data, log_level=log_level, dry_run=dry_run)
	genre = collective.Genre(artist, {
			"create_time": "2023-08-16 04:33:32.496440",
			"update_time": "2023-08-16 04:33:32.496440",
			
			"artist_id": "floris_arntzenius",
			"name": "landscape",
			"aspect_ratios": [
				"1:square"
			],
			"locations": [
				"1:Netherlands"
			],
			"methods": [
				"1:watercolor illustration",
				"1:watercolor painting",
				"1:oil painting"
			],
			"modifiers": [],
			"styles": [],
			"subjects": [
				"1:landscape",
				"1:portrait"
			],
			"time_period": "19th century",
			"notes": ""
		},
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, artist, data, log_level=5, dry_run=False):
		self.log_level = log_level
		self.dry_run = dry_run
		self.ui = moses_common.ui.Interface()
		self.artist = artist
		self.collective = artist.collective
		self.data = data
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def data(self):
		return self._data
	
	@data.setter
	def data(self, new_data):
		self._data = self.check_genre_data(new_data)
	
	@property
	def artist_id(self):
		return self.data.get('artist_id')
	
	@property
	def name(self):
		return self.data.get('name')
	
	@property
	def aspect_ratio(self):
		ratios = self.data.get('aspect_ratios')
		if type(ratios) is list:
			index = random.randrange(len(ratios))
			return ratios[index]
		return 1
	
	@property
	def location(self):
		return self.collective.choose_category(self.data.get('locations'))
	
	@property
	def method(self):
		return self.collective.choose_category(self.data.get('methods'))
	
	@property
	def methods(self):
		if 'methods' in self.data and self.data.get('methods'):
			return self.data.get('methods')
		return []
	
	@property
	def modifier(self):
		return self.collective.choose_category(self.data.get('modifiers'))
	
	@property
	def style(self):
		return self.collective.choose_category(self.data.get('styles'))
	
	def choose_subject(self):
		return self.choose_category('subjects')
	
	@property
	def time_period(self):
		return self.data.get('time_period')
	
	def __str__(self):
		return f"<moses_common.collective.Genre {self.artist_id} {self.name}>"
	
	def __repr__(self):
		return f"<moses_common.collective.Genre {self.artist_id} {self.name}>"
	
	def check_genre_data(self, data):
		if not data.get('artist_id') or not data.get('name'):
			return None
		
		new_data = {}
		text_fields = ['artist_id', 'name', 'time_period']
		for field in text_fields:
			if field in data and type(data[field]) is str:
				new_data[field] = data[field].strip()
		
		# Weighted lists
		array_fields = ['methods', 'styles', 'subjects', 'modifiers', 'locations']
		for field in array_fields:
			if field in data and type(data[field]) is list:
				new_array = []
				for element in data[field]:
					if self.is_genre_attr(element):
						new_array.append(element.strip())
				new_data[field] = new_array
		# Float lists
		array_fields = ['aspect_ratios']
		for field in array_fields:
			if field in data and type(data[field]) is list:
				new_array = []
				for element in data[field]:
					if common.is_float(element):
						new_array.append(element)
				new_data[field] = new_array
		# Text lists
		array_fields = ['notes']
		for field in array_fields:
			if field in data and type(data[field]) is list:
				new_array = []
				for element in data[field]:
					new_array.append(element)
				new_data[field] = new_array
		return new_data
	
	def is_genre_attr(self, value):
		value = value.strip()
		if re.match(r'\d+:.*$', value):
			return True
		elif re.match(r'\d+:$', value):
			return True
		elif re.search(r':', value):
			return False
		elif value:
			return True
		return False
	
	def choose_category(self, cat_name):
		tags = self.data.get(cat_name)
		if tags and re.match(r'\!', tags[0]):
# 			print("tags {}: {}".format(type(tags), tags))
			prompt = tags.pop(0)
# 			print("prompt {}: {}".format(type(prompt), prompt))
			if not tags:
				# Generate new tags
				if not self.collective.gpt:
					self.collective.gpt = moses_common.openai.GPT(openai_api_key=self.collective.openai_api_key, log_level=self.log_level, dry_run=self.dry_run)
				results = self.collective.gpt.chat(prompt)
				tags = self.collective.gpt.process_list(results)
			
			tag = tags.pop(0)
# 			print("tag {}: {}".format(type(tag), tag))
			tags.insert(0, prompt)
# 			print("tags {}: {}".format(type(tags), tags))
			self.data[cat_name] = tags
			self.save()
			return tag
			
		return self.collective.choose_category(self.data.get(cat_name))
	
	def save(self):
		genre = None
		for g in self.artist.genres:
			if self.name in g.name:
				genre = g
				break
		
		success = False
		if genre:
			# Update
			self.ui.body(f"Update {self.artist_id}-{self.name}")
			success = genres_table.update_item(self.data)
		
		else:
			# Insert
			self.ui.body(f"Insert {self.artist_id}-{self.name}")
			# Get ratios
			results = self.artist.get_search_results(20)
			ratios = []
			for result in results:
				ratio = common.round_half_up(common.convert_to_int(result['image']['width']) / common.convert_to_int(result['image']['height']), 2)
				ratios.append(ratio)
			self.data['aspect_ratios'] = ratios
			success = genres_table.put_item(self.data)
		
		if not success:
			return False
		
		self.collective.set_genres_update()
		return True
	
	def delete(self):
		self.ui.body(f"Delete {self.artist_id}-{self.name}")
		success = genres_table.delete_item(self.artist_id, self.name)
		
		if not success:
			return False
		
		self.collective.set_genres_update()
		return True
	
	
	def get_prompt(self):
		data = {
			"query": {}
		}
		prompt_list = []
		
		# Method
		main = "A painting"
		method = self.method
		if method:
			data['method'] = method
			if re.search(r'\bart$', self.method):
				main = f"{self.method}"
			elif re.match(r'[aeiou]', self.method):
				main = f"An {self.method}"
			else:
				main = f"A {self.method}"
		
		# Artist
		main += f" in the style of {self.artist.name}"
		prompt_list.append(main)
		data['query']['artist_name'] = self.artist.name
		data['query']['artist_id'] = self.artist_id
		
		# Subject
		subject = self.choose_subject()
		if subject:
			subject = re.sub(r'[,\.]+\s*$', '', subject)
			data['query']['subject'] = subject
			prompt_list.append(subject)
		
		# Style
		style = self.style
		if style:
			data['query']['style'] = style
			prompt_list.append(style)
		
		# Locations
		location = self.location
		if location:
			data['query']['location'] = location
			prompt_list.append(location)
		
		# Modifiers
		modifier = self.modifier
		if modifier:
			data['query']['modifier'] = modifier
			prompt_list.append(modifier)
		
		# Time period
		if self.time_period:
			data['query']['time_period'] = self.time_period
			prompt_list.append(self.time_period)
		
		# Aspect ratios
		data['aspect_ratio'] = self.aspect_ratio
		
# 		data['orientation'] = 'square'
# 		data['aspect'] = 'square'
# 		if re.search(r'-', aspect_ratio):
# 			parts = aspect_ratio.split('-')
# 			data['orientation'] = parts[0]
# 			data['aspect'] = parts[1]
		
		data['negative_prompt'] = self.get_negative_prompt(data)
		
		data['prompt'] = prompt_list.pop(0) + '. '
		data['prompt'] += ', '.join(prompt_list)
		return data
	
	def get_negative_prompt(self, data):
		nps = []
		method = data.get('method')
		if self.name == 'portrait':
			nps.append('nude, nsfw, extra fingers, mutated hands, ((poorly drawn hands)), (malformed limbs), ((missing arms)), ((missing legs)), (((extra arms))), (((extra legs))), mutated hands, (fused fingers), (too many fingers), (((long neck)))')
		if method and method == 'photograph':
			nps.append('illustration, painting, drawing, art, sketch')
		if method and re.search(r'\b(illustration|painting)\b', method, re.IGNORECASE):
			nps.append('3d, frame, border, wall, hanging, border, canvas')
		nps.append('lowres, error, cropped, worst quality, low quality, jpeg artifacts, out of frame, watermark, signature')
		return ', '.join(nps)





class Image:
	"""
	image = collective.Image(data, log_level=log_level, dry_run=dry_run)
	image = collective.Image({
			"filename": "arnold_bocklin-1691785366-sdxl10-411381112-portrait.png",
			"create_time": "2023-08-11 20:22:46.477604",
			"aspect": "square",
			"cfg_scale": 7,
			"engine_label": "Stable Diffusion XL 1.0",
			"engine_name": "sdxl10",
			"height": 1152,
			"image_url": "https://artintelligence.gallery/images/arnold_bocklin-1691785366-sdxl10-411381112-portrait.png",
			"negative_prompt": "nude, nsfw, duplication artifact, ((((ugly)))), (((duplicate))), ((morbid)), ((mutilated)), out of frame, extra fingers, mutated hands, ((poorly drawn hands)), ((poorly drawn face)), (((mutation))), (((deformed))), ((ugly)), blurry, ((bad anatomy)), (((bad proportions))), ((extra limbs)), cloned face, (((disfigured))), out of frame, ugly, extra limbs, (bad anatomy), gross proportions, (malformed limbs), ((missing arms)), ((missing legs)), (((extra arms))), (((extra legs))), mutated hands, (fused fingers), (too many fingers), (((long neck))), 3d, concept art, frame, border, wall, hanging, border, canvas, deformed, disfigured, underexposed, overexposed, lowres, error, cropped, worst quality, low quality, jpeg artifacts, out of frame, watermark, signature",
			"nsfw": false,
			"orientation": "portrait",
			"prompt": "Arnold B\u00f6cklin's oil painting, created in the 19th century and embracing the mystical style of symbolism, evokes a deep sense of introspection and enigmatic allure. The portrait captures an ethereal figure, shrouded in an air of mysticism, emanating from the mysterious shadows that surround them. With delicate brushstrokes, B\u00f6cklin explores the inner world of the sitter, conveying a profound sense of introspection through symbolic imagery and rich, emotive colors. The painting invites viewers to delve into the hidden depths of the subject's soul, leaving them captivated by the enigmatic beauty that lies within.",
			"query": "Generate a short description of an oil painting by Arnold B\u00f6cklin of a portrait in the style of symbolism from the 19th century.",
			"query-artist": "Arnold B\u00f6cklin",
			"query-artist_id": "arnold_bocklin",
			"query-artist_name": "Arnold B\u00f6cklin",
			"query-art_form": "painting",
			"query-century": "19th century",
			"query-method": "oil",
			"query-style": "symbolism",
			"query-subject": "a portrait",
			"query-time_period": "19th century",
			"score": 3,
			"seed": 411381112,
			"steps": 30,
			"width": 896
		},
		log_level = 5,
		dry_run = False
	)
	"""
	def __init__(self, data, log_level=5, dry_run=False):
		self.log_level = log_level
		self.dry_run = dry_run
		self.ui = moses_common.ui.Interface()
		self.data = data
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def data(self):
		return self._data
	
	@data.setter
	def data(self, new_data):
		self._data = new_data
	
	@property
	def artist_id(self):
		return self.data.get('query-artist_id')
	
	@property
	def image_url(self):
		return self.data.get('image_url')
	
	@property
	def width(self):
		return self.data.get('width')
	
	@property
	def height(self):
		return self.data.get('height')
	
