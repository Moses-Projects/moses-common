# print("Loaded S3 module")

import moses_common.__init__ as common
import base64
import io
import json
from boto3 import client as boto3_client

class Bucket:
	"""
	import moses_common.s3
	bucket = moses_common.s3.Bucket(bucket_name)
	"""
	def __init__(self, bucket_name, log_level=5, dry_run=False):
		self.log_level = log_level
		self._dry_run = dry_run
		self._client = boto3_client('s3', region_name="us-west-2")
		self._bucket_name = bucket_name
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def name(self):
		return self._bucket_name
	
	

class Object:
	"""
	import moses_common.s3
	file = moses_common.s3.Object(object_name)
	"""
	def __init__(self, bucket, object_name, log_level=5, dry_run=False):
		self.log_level = log_level
		self._dry_run = dry_run
		self._bucket = bucket
		self._client = self._bucket._client
		self._object_name = object_name
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	"""
	response = file.get_file(filepath=None)
	"""
	def get_file(self, filepath):
		response = self._client.get_object(
			Bucket = self._bucket.name,
			Key = self._object_name
		)
		if type(response) is dict and 'Body' in response:
			if filepath:
				common.write_file(filepath, response['Body'])
				return True
			else:
				return response['Body']
		return None
	
	"""
	url = file.get_presigned_url(expiration_time=3600)
	"""
	def get_presigned_url(self, expiration_time=3600):
		try:
			# Generate a presigned URL for the S3 object
			presigned_url = self._client.generate_presigned_url(
				'get_object',
				Params={
					'Bucket': self._bucket.name,
					'Key': self._object_name
				},
				ExpiresIn=expiration_time
			)
			
			return presigned_url
		
		except Exception as e:
			print(f"Error generating presigned URL: {e}")
			return None
	
	"""
	response = file.upload_file(filepath)
	"""
	def upload_file(self, filepath):
		response = self._client.upload_file(
			filepath,
			self._bucket.name,
			self._object_name
		)
	
	"""
	response = file.upload_content(content)
	"""
	def upload_content(self, content):
		response = self._client.upload_fileobj(
			io.BytesIO(content),
			self._bucket.name,
			self._object_name
		)
	