# print("Loaded Cognito module")

import moses_common.__init__ as common
import moses_common.ui


class User:
	"""
	import moses_common.cognito

	user = moses_common.cognito.User(app, log_level=log_level, dry_run=dry_run)
	user = moses_common.cognito.User({
		"app_name": app_name,
		"auth_hostname": auth_hostname,
		"client_id": client_id
	}, log_level=log_level, dry_run=dry_run)
	"""
	def __init__(self, app, log_level=5, dry_run=False):
		self.dry_run = dry_run
		self.log_level = log_level
		self.ui = moses_common.ui.Interface()

		self._app = app
		self.token_url = None
		self.user_info_url = None
		if self.auth_hostname:
			self.token_url = f"https://{self.auth_hostname}/oauth2/token"
			self.user_info_url = f"https://{self.auth_hostname}/oauth2/userInfo"

		self._jwt = None
		self._info = None


	@property
	def log_level(self):
		return self._log_level

	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)

	@property
	def client_id(self):
		if self._app and type(self._app) is dict:
			return self._app.get('client_id')
		return None

	@property
	def client_secret(self):
		if self._app and type(self._app) is dict:
			return self._app.get('client_secret')
		return None

	@property
	def auth_hostname(self):
		if self._app and type(self._app) is dict:
			return self._app.get('auth_hostname')
		return None

	@property
	def jwt(self):
		if self._jwt:
			return self._jwt.copy()
		return None

	@jwt.setter
	def jwt(self, value):
		if type(value) is not dict or 'access_token' not in value or 'refresh_token' not in value:
			return
		self._jwt = value

	@property
	def access_token(self):
		if self._jwt:
			return self._jwt.get('access_token')
		return None

	@access_token.setter
	def access_token(self, value):
		if type(value) is str:
			if not self._jwt:
				self._jwt = {}
			self._jwt['access_token'] = value

	@property
	def refresh_token(self):
		if self._jwt:
			return self._jwt.get('refresh_token')
		return None

	@refresh_token.setter
	def refresh_token(self, value):
		if type(value) is str:
			if not self._jwt:
				self._jwt = {}
			self._jwt['refresh_token'] = value

	@property
	def info(self):
		if self._info:
			return self._info.copy()
		return None

	@property
	def username(self):
		if self._info:
			return self._info.get('username')
		return None

	@property
	def sub(self):
		if self._info:
			return self._info.get('sub')
		return None

	"""
	success, response = user.get_tokens_from_code(code, redirect_uri)
	"""
	def get_tokens_from_code(self, code, redirect_uri):
		data = {
			"grant_type": "authorization_code",
			"client_id": self.client_id,
			"code": code,
			"redirect_uri": redirect_uri
		}

		response_code, response_data = common.get_url(self.token_url, {
			"method": "POST",
			"username": self.client_id,
			"password": self.client_secret,
			"headers": {
				"Content-Type": "application/x-www-form-urlencoded"
			},
			"data": data
		}, log_level=self.log_level, dry_run=self.dry_run)

		if self.dry_run:
			self.jwt = {
				"access_token": "access-xxxxx",
				"refresh_token": "refresh-xxxxx",
				"id_token": "id-xxxxx"
			}
			return True, self.jwt

		if response_code != 200:
			return False, f"{response_code} {response_data}"
		if type(response_data) is dict and 'access_token' in response_data:
			self.jwt = response_data
			return True, self.jwt
		return False, "Unexpected response"


	"""
	success, response = user.refresh_access_token()
	"""
	def refresh_access_token(self):
		if not self.refresh_token:
			return False, "No refresh token"

		data = {
			"grant_type": "refresh_token",
			"client_id": self.client_id,
			"refresh_token": self.refresh_token
		}

		response_code, response_data = common.get_url(self.token_url, {
			"method": "POST",
			"username": self.client_id,
			"password": self.client_secret,
			"headers": {
				"Content-Type": "application/x-www-form-urlencoded"
			},
			"data": data
		}, log_level=self.log_level, dry_run=self.dry_run)

		if self.dry_run:
			self.access_token = "access-xxxxx-refreshed"
			return True, {
				"access_token": self.access_token,
				"id_token": "id-xxxxx",
				"token_type": "Bearer",
				"expires_in": 3600
			}

		print("response_data {}: {}".format(type(response_data), response_data))
		if response_code != 200:
			return False, f"{response_code} {response_data}"
		if type(response_data) is dict and 'access_token' in response_data:
			self.access_token = response_data['access_token']
			return True, response_data
		return False, "Unexpected response"

	"""
	user_info = user.get_user_from_access_token()
	
	{
		"sub": "a38e7dd2-xxx",
		"email_verified": "false",
		"email": "qnqdrdyx2r@privaterelay.appleid.com",
		"username": "signinwithapple_001124.69bbda7a9d4c4289834295cd7e3198fd.2208"
	}
	"""
	def get_user_from_access_token(self):
		if not self.access_token:
			return False, "No access token"
		response_code, response_data = common.get_url(self.user_info_url, {
			"bearer_token": self.access_token,
			"headers": {
				"Content-Type": "application/x-www-form-urlencoded"
			}
		}, log_level=self.log_level, dry_run=self.dry_run)

		if self.dry_run:
			self._info = {
				"sub": "sub-xxxxx",
				"email_verified": "false",
				"email": "qnqdrdyx2r@example.com",
				"username": "username-xxxxx"
			}
			return True, self._info

		if response_code != 200:
			return False, f"{response_code} {response_data}"
		elif type(response_data) is dict and 'username' in response_data:
			self._info = response_data
			return True, self._info
		return False, "Unexpected response"
