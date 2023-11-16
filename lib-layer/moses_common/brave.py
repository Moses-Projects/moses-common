# print("Loaded Brave module")

import os
import re

import moses_common.__init__ as common
import moses_common.ui


"""
import moses_common.brave
"""
class ImageSearch:
	"""
	image_search = moses_common.brave.ImageSearch(api_key=None, log_level=log_level, dry_run=dry_run)
	"""
	def __init__(self, api_key=None, log_level=5, dry_run=False):
		self.dry_run = dry_run
		self.log_level = log_level
		self.ui = moses_common.ui.Interface()
		
		self.api_key = api_key or os.environ.get('BRAVE_API_KEY')
		if not self.api_key:
			raise KeyError("A Brave API key is required from https://api.search.brave.com. It can be passed as an arg or set as BRAVE_API_KEY env var.")
		
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	
	def search(self, search_string, limit=8, debug=True):
		url = 'https://api.search.brave.com/res/v1/images/search?'
		query = {
			"safesearch": "off",
			"count": limit,
			"search_lang": "en",
			"country": "us",
			"spellcheck": "1",
			"q": search_string
		}
		url += common.url_encode(query)
		if self.log_level >= 5:
			print("url {}: {}".format(type(url), url))
		response_code, response_data = common.get_url(url, {
			"headers": {
				"Accept": "application/json",
				"Accept-Encoding": "gzip",
				"X-Subscription-Token": self.api_key
			}
		})
		
		if self.log_level >= 5:
			print("response_data {}: {}".format(type(response_data), response_data))
		if response_code != 200:
			self.ui.error(f"Failed with error {response_code} {response_data}")
			return None
# 		if type(response_data) is dict and 'username' in response_data:
# 			return response_data
		return None
	
	
