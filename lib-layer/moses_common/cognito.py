# print("Loaded Cognito module")

import datetime
import re
from boto3 import client as boto3_client

import moses_common.__init__ as common
import moses_common.ui


"""
import moses_common.cognito
"""
class User:
	"""
	user = moses_common.cognito.User(username=None, access_token=None, log_level=log_level, dry_run=dry_run)
	"""
	def __init__(self, username, access_token, log_level=5, dry_run=False):
		self.dry_run = dry_run
		self.log_level = log_level
		self.ui = moses_common.ui.Interface()
		
		self.client = boto3_client('cognito-idp', region_name="us-west-2")
		
		self._info = self.get_user_from_access_token(access_token)
		
	
# 	def get_user_from_access_token(self, access_token):
# 		try:
# 			response = self.client.get_user(
# 				AccessToken = access_token
# 			)
# 		except self.client.exceptions.NotAuthorizedException as e:
# 			print("error: {}".format(e))
# 			raise e
# 		else:
# 			print("response {}: {}".format(type(response), response))
# 			if common.is_success(response) and 'Username' in response:
# 				self._info = response
# 				return True
# 			return False
	
	def get_user_from_access_token(self, access_token, debug=True):
		if not access_token:
			return None
		url = 'https://auth.artintelligence.gallery/oauth2/userInfo';
		response_code, response_data = common.get_url(url, {
			"bearer_token": access_token,
			"headers": {
				"Content-Type": "application/x-www-form-urlencoded"
			}
		})
		
# 		print("response_data {}: {}".format(type(response_data), response_data))
		if response_code != 200:
			self.ui.error(f"Failed with error {response_code} {response_data}")
			return None
		if type(response_data) is dict and 'username' in response_data:
			return response_data
		return None
	
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def exists(self):
		if self._info and type(self._info) is dict and self._info.get('username'):
			return True
		return False
	
	@property
	def username(self):
		if self.exists:
			return self._info.get('username')
		return None
	
	@property
	def sub(self):
		if self.exists:
			return self._info.get('sub')
		return None
	
	@property
	def data(self):
		return self._info
	
