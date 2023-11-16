# print("Loaded Google Search module")

import os
import re

import moses_common.__init__ as common
import moses_common.ui


"""
import moses_common.google_search
"""
class ImageSearch:
	"""
	image_search = moses_common.google_search.ImageSearch(api_key=None, project_cx=None, log_level=log_level, dry_run=dry_run)
	"""
	def __init__(self, api_key=None, project_cx=None, log_level=5, dry_run=False):
		self.dry_run = dry_run
		self.log_level = log_level
		self.ui = moses_common.ui.Interface()
		
		self.api_key = api_key or os.environ.get('GOOGLE_SEARCH_API_KEY')
		self.project_cx = project_cx or os.environ.get('GOOGLE_SEARCH_PROJECT_CX')
		if not self.api_key:
			raise KeyError("A Google Search API key is required. It can be passed as an arg or set as GOOGLE_SEARCH_API_KEY env var.")
		if not self.project_cx:
			raise KeyError("A Google Search project CX is required. It can be passed as an arg or set as GOOGLE_SEARCH_PROJECT_CX env var.")
		
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	"""
	[
		{
			"kind": "customsearch#result",
			"title": "Arnold B\u00f6cklin | Symbolist, Landscapes, Mythology | Britannica",
			"htmlTitle": "<b>Arnold B\u00f6cklin</b> | Symbolist, Landscapes, Mythology | Britannica",
			"link": "https://cdn.britannica.com/13/166213-050-2D373B47/Sanctuary-of-Hercules-oil-wood-Arnold-Bocklin-1884.jpg",
			"displayLink": "www.britannica.com",
			"snippet": "Arnold B\u00f6cklin | Symbolist, Landscapes, Mythology | Britannica",
			"htmlSnippet": "<b>Arnold B\u00f6cklin</b> | Symbolist, Landscapes, Mythology | Britannica",
			"mime": "image/jpeg",
			"fileFormat": "image/jpeg",
			"image": {
				"contextLink": "https://www.britannica.com/biography/Arnold-Bocklin",
				"height": 752,
				"width": 1200,
				"byteSize": 172844,
				"thumbnailLink": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQP2i1dpWoXS_YnGubry2MTFVOBqHPUTlaSzIbtUw_G6LZmjjy2dlUtBQ&s",
				"thumbnailHeight": 94,
				"thumbnailWidth": 150
			}
		}, ...
	]
	"""
	def search(self, search_string, limit=8, debug=True):
		url = "https://www.googleapis.com/customsearch/v1?"
		if limit > 10:
			limit = 10
		query = {
			"key": self.api_key,
			"cx": self.project_cx,
			"num": limit,
			"searchType": "image",
			"q": search_string
		}
		url += common.url_encode(query)
		if self.log_level >= 6:
			print("url {}: {}".format(type(url), url))
		response_code, response_data = common.get_url(url, {
			"headers": {
				"Accept": "application/json",
				"Accept-Encoding": "gzip"
			}
		})
		
		if self.log_level >= 6:
			print("response_data {}: {}".format(type(response_data), response_data))
		if response_code != 200:
			self.ui.error(f"Failed with error {response_code} {response_data}")
			return None
		if type(response_data) is dict and 'items' in response_data:
			return response_data['items']
		return []
	
