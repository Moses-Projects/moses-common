# print("Loaded Secrets Manager module")

import base64
import json
from botocore.exceptions import ClientError
from boto3 import client as boto3_client

import moses_common.__init__ as common
import moses_common.ui

class Secrets:
	"""
	import moses_common.secrets_manager
	secrets = moses_common.secrets_manager.Secrets()
	"""
	def __init__(self, log_level=5, dry_run=False):
		self.dry_run = dry_run
		self.log_level = log_level
		self.ui = cg_shared.ui.Interface()
		self.client = boto3_client('secretsmanager', region_name="us-west-2")
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	"""
	secret = secrets.get(secret_name)
	"""
	def get(self, secret_name):
		# See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
		try:
			response = self.client.get_secret_value(
				SecretId = secret_name
			)
		except ClientError as e:
			if e.response['Error']['Code'] == 'DecryptionFailureException':
				# Secrets Manager can't decrypt the protected secret text using the provided KMS key.
				raise e
			elif e.response['Error']['Code'] == 'InternalServiceErrorException':
				# An error occurred on the server side.
				raise e
			elif e.response['Error']['Code'] == 'InvalidParameterException':
				# You provided an invalid value for a parameter.
				raise e
			elif e.response['Error']['Code'] == 'InvalidRequestException':
				# You provided a parameter value that is not valid for the current state of the resource.
				raise e
			elif e.response['Error']['Code'] == 'ResourceNotFoundException':
				# We can't find the resource that you asked for.
				return False
			elif e.response['Error']['Code'] == 'AccessDeniedException':
				# The Lambda function does not have permissions to access the secret.
				raise e
			raise e
		else:
			# Decrypts secret using the associated KMS CMK.
			# Depending on whether the secret is a string or binary, one of these fields will be populated.
			if 'SecretString' in response:
				secret = response['SecretString']
				return json.loads(secret)
			else:
				decoded_binary_secret = base64.b64decode(response['SecretBinary'])
				return json.loads(decoded_binary_secret)

"""
create_secret()
delete_secret()
describe_secret()
get_random_password()
get_secret_value()
put_secret_value()
update_secret()
"""
class Secret:
	"""
	import moses_common.secrets_manager
	secret = moses_common.secrets_manager.Secret(secret_name)
	"""
	def __init__(self, secret_name, log_level=5, dry_run=False):
		self.dry_run = dry_run
		self.log_level = log_level
		self.ui = cg_shared.ui.Interface()
		self.client = boto3_client('secretsmanager', region_name="us-west-2")
		self.name = secret_name
		self.info = self.load()
		if self.info and type(self.info) is dict:
			self.exists = True
		else:
			self.exists = False
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	"""
	secret_info = secret.load()
	"""
	def load(self):
		# See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
		try:
			response = self.client.describe_secret(
				SecretId = self.name
			)
		except ClientError as e:
			if e.response['Error']['Code'] == 'ResourceNotFoundException':
				# We can't find the resource that you asked for.
				return False
			raise e
		else:
# 			print("response {}: {}".format(type(response), response))
			if common.is_success(response) and type(response) is dict and 'Name' in response:
				return response
			return False
	
	def get_arn(self):
		if not self.exists:
			return False
		return self.info['ARN']
	
	def get_name(self):
		if not self.exists:
			return False
		return self.info['Name']
	
	"""
	secret_value = secret.get_value()
	"""
	def get_value(self):
		if not self.exists:
			return None
		response = self.client.get_secret_value(
			SecretId = self.name
		)
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'SecretString' in response:
			secret = response['SecretString']
			return json.loads(secret)
		else:
			decoded_binary_secret = base64.b64decode(response['SecretBinary'])
			return json.loads(decoded_binary_secret)
		
	
	"""
	password = secret.get_random_password()
	password = secret.get_random_password(length)
	"""
	def get_random_password(self, length=32, alpha_only=False):
		response = None
		if alpha_only:
			response = self.client.get_random_password(
				PasswordLength = length,
				ExcludeNumbers = False,
				ExcludePunctuation = True,
				ExcludeUppercase = False,
				ExcludeLowercase = False,
				IncludeSpace = False,
				RequireEachIncludedType = True
			)
		else:
			response = self.client.get_random_password(
				PasswordLength = length,
				ExcludeCharacters = '@%\\:\"\'/\`',
				ExcludeNumbers = False,
				ExcludePunctuation = False,
				ExcludeUppercase = False,
				ExcludeLowercase = False,
				IncludeSpace = False,
				RequireEachIncludedType = True
			)
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'RandomPassword' in response:
			return response['RandomPassword']
		return False
		
	
	"""
	response = secret.create(args)
	"""
	def create(self, args):
		if not args or type(args) is not dict:
			raise AttributeError("Received invalid args.")
		
		secret_string = json.dumps(args['value'])
		
		tags_list = []
		if 'tags' in args:
			tags_list = common.convert_tags(args['tags'], 'upper')
		
		description = ''
		if 'description' in args:
			description = args['description']
		
		if self.dry_run:
			self.ui.dry_run(f"create_secret('{self.name}', '{secret_string}')")
			self.exists = True
			return True
		
		response = self.client.create_secret(
			Name = self.name,
			Description = description,
			SecretString = secret_string,
			Tags = tags_list
		)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'Name' in response and type(response['Name']) is str:
			self.info = response
			self.exists = True
			return self.get_arn()
		return False
	
	"""
	response = secret.put_value(value)
	"""
	def put_value(self, value):
		if not value or type(value) is not dict:
			raise TypeError("value should be type dict")
		
		secret_string = json.dumps(value)
		
		if self.dry_run:
			self.ui.dry_run(f"put_secret_value('{self.name}', '{secret_string}')")
			self.exists = True
			return True
		
		response = self.client.put_secret_value(
			SecretId = self.get_arn(),
			SecretString = secret_string
		)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'Name' in response and type(response['Name']) is str:
			return True
		return False
	
	"""
	response = secret.delete()
	"""
	def delete(self):
		if self.dry_run:
			self.ui.dry_run(f"delete_secret('{self.name}')")
			self.info = False
			self.exists = False
			return True
		
		response = self.client.delete_secret(
			SecretId = self.get_arn(),
			ForceDeleteWithoutRecovery = True
		)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'Name' in response and type(response['Name']) is str:
			self.info = False
			self.exists = False
			return True
		return False
