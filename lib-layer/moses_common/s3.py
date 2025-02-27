# print("Loaded S3 module")

import base64
import io
import json
from boto3 import client as boto3_client

import moses_common.__init__ as common
import moses_common.ui

class Bucket:
	"""
	import moses_common.s3
	bucket = moses_common.s3.Bucket(bucket_name)
	"""
	def __init__(self, bucket_name, log_level=5, dry_run=False):
		self.log_level = log_level
		self.dry_run = dry_run
		self.client = boto3_client('s3', region_name="us-west-2")
		self.bucket_name = bucket_name
		self.ui = moses_common.ui.Interface(use_slack_format=True)
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def name(self):
		return self.bucket_name
	
	

class Object:
	"""
	import moses_common.s3
	file = moses_common.s3.Object(bucket, object_name)
	"""
	def __init__(self, bucket, object_name, log_level=5, dry_run=False):
		self.log_level = log_level
		self.dry_run = dry_run
		self.bucket = bucket
		self.client = self.bucket.client
		self.object_name = object_name
		self.ui = moses_common.ui.Interface(use_slack_format=True)
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	"""
	response = file.get_file(filepath=None)
	"""
	def get_file(self, filepath=None):
		response = None
		if self.dry_run:
			self.ui.dry_run(f"s3.get_object('{self.bucket.name}', '{self.object_name}')")
			return True
		try:
			response = self.client.get_object(
				Bucket = self.bucket.name,
				Key = self.object_name
			)
		except NoCredentialsError:
			print("Error: AWS credentials not found.")
		except PartialCredentialsError:
			print("Error: Incomplete AWS credentials.")
		except ClientError as e:
			# Handle specific client errors
			if e.response['Error']['Code'] == 'NoSuchKey':
# 				print(f"Error: The object '{self.object_name}' does not exist in bucket '{self.bucket.name}'.")
				return None
			elif e.response['Error']['Code'] == 'NoSuchBucket':
				print(f"Error: The bucket '{self.bucket.name}' does not exist.")
				return None
			else:
				print(f"ClientError: {e.response['Error']['Message']}")
		except Exception as e:
			print(f"An unexpected error occurred: {str(e)}")
		
		if type(response) is dict and 'Body' in response:
			if filepath:
				common.write_file(filepath, response['Body'])
				return True
			else:
				content = response['Body'].read().decode('utf-8')
				return content
		return None
	
	"""
	url = file.get_presigned_url(expiration_time=3600)
	"""
	def get_presigned_url(self, expiration_time=3600):
		try:
			# Generate a presigned URL for the S3 object
			presigned_url = self.client.generate_presigned_url(
				'get_object',
				Params={
					'Bucket': self.bucket.name,
					'Key': self.object_name
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
		if self.dry_run:
			self.ui.dry_run(f"s3.upload_file('{filepath}', '{self.bucket.name}', '{self.object_name}')")
			return True
		response = self.client.upload_file(
			filepath,
			self.bucket.name,
			self.object_name
		)
		return True
	
	"""
	response = file.upload_content(content)
	"""
	def upload_content(self, content):
		if self.dry_run:
			self.ui.dry_run(f"s3.upload_fileobj('{self.bucket.name}', '{self.object_name}')")
			return True
		if type(content) is str:
		    response = self.client.put_object(
				Body=content,
				Bucket=self.bucket.name,
				Key=self.object_name
			)
		else:
			response = self.client.upload_fileobj(
				io.BytesIO(content),
				self.bucket.name,
				self.object_name
			)
		return True
	
