# print("Loaded S3 module")

import base64
import io
import json
from boto3 import client as boto3_client
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

import moses_common.__init__ as common
import moses_common.ui

class Bucket:
	"""
	import moses_common.s3
	bucket = moses_common.s3.Bucket(bucket_name)
	"""
	def __init__(self, bucket_name, ui=None, dry_run=False):
		self.dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
		self.client = boto3_client('s3', region_name="us-west-2")
		self.bucket_name = bucket_name
		self.info = self.load()
		if self.info and type(self.info) is dict:
			self.exists = True
		else:
			self.exists = False
	
	"""
	s3_bucket_info = s3_bucket.load()
	"""
	def load(self):
		response = self.client.list_buckets()
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'Buckets' in response and type(response['Buckets']) is list:
			for bucket in response['Buckets']:
				if bucket['Name'] == self.name:
					return bucket
		return None
	
	@property
	def name(self):
		return self.bucket_name
	
	def get_arn(self):
		if not self.exists:
			return False
		return "arn:aws:s3:::{}".format(self.info['Name'])
	
	def get_name(self):
		if not self.exists:
			return False
		return self.info['Name']
	
	def get_cors_rules(self):
		try:
			response = self.client.get_bucket_cors(
				Bucket = self.name
			)
		except ClientError as e:
			if e.response['Error']['Code'] == 'NoSuchCORSConfiguration':
				# We can't find the resource that you asked for.
				return None
			raise e
		else:
# 			print("response {}: {}".format(type(response), response))
			if common.is_success(response) and 'CORSRules' in response and type(response['CORSRules']) is list:
				return response['CORSRules']
		return None
	
	def get_crossdomain(self):
		try:
			response = self.client.get_object(
				Bucket = self.name,
				Key = 'crossdomain.xml'
			)
		except ClientError as e:
			if e.response['Error']['Code'] == 'NoSuchKey':
				# We can't find the resource that you asked for.
				return None
			raise e
		else:
# 			print("response {}: {}".format(type(response), response))
			if common.is_success(response) and 'Body' in response:
				body = response['Body'].read()
				body_string = body.decode('utf-8')
				return body_string
		return None
	
	def get_crossdomain_domains(self):
		xml_string = self.get_crossdomain()
		if not xml_string or type(xml_string) is not str:
			return None
		xml_object = ET.fromstring(xml_string)
		domains = []
		for xml_child in xml_object:
			if xml_child.tag == 'allow-access-from':
				domains.append(xml_child.attrib['domain'])
		return domains
	
	def get_logging(self):
		try:
			response = self.client.get_bucket_logging(
				Bucket = self.name
			)
		except ClientError as e:
			if e.response['Error']['Code'] == 'NoSuchBucketPolicy':
				# We can't find the resource that you asked for.
				return None
			raise e
		else:
# 			print("response {}: {}".format(type(response), response))
			if common.is_success(response) and 'LoggingEnabled' in response and type(response['LoggingEnabled']) is dict:
				return response['LoggingEnabled']
		return None
	
	def get_text_file(self, path):
		try:
			response = self.client.get_object(
				Bucket = self.name,
				Key = path
			)
		except ClientError as e:
			if e.response['Error']['Code'] == 'NoSuchBucketPolicy':
				# We can't find the resource that you asked for.
				return None
			if e.response['Error']['Code'] == 'NoSuchKey':
				# We can't find the resource that you asked for.
				return None
			raise e
		else:
# 			print("response {}: {}".format(type(response), response))
			if common.is_success(response) and 'Body' in response:
				content = response['Body'].read().decode('utf-8')
				return content
		return None
	
	def get_policy(self):
		try:
			response = self.client.get_bucket_policy(
				Bucket = self.name
			)
		except ClientError as e:
			if e.response['Error']['Code'] == 'NoSuchBucketPolicy':
				# We can't find the resource that you asked for.
				return None
			raise e
		else:
# 			print("response {}: {}".format(type(response), response))
			if common.is_success(response) and 'Policy' in response and type(response['Policy']) is str and common.is_json(response['Policy']):
				return json.loads(response['Policy'])
		return None
	
	def get_tags(self):
		try:
			response = self.client.get_bucket_tagging(
				Bucket = self.name
			)
		except ClientError as e:
			if e.response['Error']['Code'] == 'NoSuchTagSet':
				# We can't find the resource that you asked for.
				return None
			raise e
		else:
# 			print("response {}: {}".format(type(response), response))
			if common.is_success(response) and 'TagSet' in response and type(response['TagSet']) is list:
				return response['TagSet']
		return None
	
	"""
	response = s3_bucket.create()
	"""
	def create(self):
		if self.dry_run:
			self.ui.dry_run(f"s3.create_bucket('{self.name}')")
			return True
		
		response = self.client.create_bucket(
			ACL = 'private',
			Bucket = self.name,
			CreateBucketConfiguration = {
				'LocationConstraint': 'us-west-2'
			}
		)
		
# 		print("response {}: {}".format(type(response), response))
		if type(response) is dict and 'Location' in response and type(response['Location']) is str:
			self.info = { 'Name': self.name }
			self.exists = True
			return True
		return False
	
	"""
	response = s3_bucket.put_cors_rules(cors_rules)
	"""
	def put_cors_rules(self, cors_rules):
		if self.dry_run:
			self.ui.dry_run(f"s3.put_bucket_cors({self.name})")
			return True
		
		response = self.client.put_bucket_cors(
			Bucket = self.name,
			CORSConfiguration = {
				'CORSRules': cors_rules
			}
		)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response):
			return True
		return False
		
	"""
	response = s3_bucket.put_crossdomain(crossdomain)
	"""
	def put_crossdomain(self, crossdomain):
		if self.dry_run:
			self.ui.dry_run(f"put_crossdomain() s3.put_object({self.name})")
			return True
		
		response = self.client.put_object(
			Body = crossdomain.encode('utf-8'),
			Bucket = self.name,
			Key = 'crossdomain.xml'
		)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response):
			return True
		return False
		
	"""
	response = s3_bucket.put_logging(target_bucket, target_prefix)
	"""
	def put_logging(self, target_bucket, target_prefix):
		if self.dry_run:
			self.ui.dry_run(f"s3.put_bucket_logging({self.name})")
			return True
		
		response = self.client.put_bucket_logging(
			Bucket = self.name,
			BucketLoggingStatus = {
				"LoggingEnabled": {
					"TargetBucket": target_bucket,
					"TargetPrefix": target_prefix
				}
			}
		)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response):
			return True
		return False
		
	"""
	response = s3_bucket.put_policy(policy)
	"""
	def put_policy(self, policy):
		if self.dry_run:
			self.ui.dry_run(f"s3.put_bucket_policy({self.name})")
			return True
		
		policy_string = policy
		if type(policy) is not str:
			policy_string = json.dumps(policy)
		response = self.client.put_bucket_policy(
			Bucket = self.name,
			Policy = policy_string
		)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response):
			return True
		return False
		
	"""
	response = s3_bucket.put_tags(tags)
	"""
	def put_tags(self, tags):
		if self.dry_run:
			self.ui.dry_run(f"s3.put_bucket_tagging({self.name})")
			return True
		
		tags_list = common.convert_dict_to_list(tags)
		response = self.client.put_bucket_tagging(
			Bucket = self.name,
			Tagging = {
				'TagSet': tags_list
			}
		)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response):
			return True
		return False
		
	
	"""
	response = s3_bucket.delete()
	"""
	def delete(self):
		if self.dry_run:
			self.ui.dry_run(f"s3.delete_bucket({self.name})")
			return True
		
		response = self.client.delete_bucket(
			UserName = self.name
		)
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response):
			self.info = False
			self.exists = False
			return True
		return False
		

class Object:
	"""
	import moses_common.s3
	file = moses_common.s3.Object(bucket, object_name)
	"""
	def __init__(self, bucket, object_name, ui=None, dry_run=False):
		self.dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
		self.bucket = bucket
		if type(bucket) is str:
			self.bucket = Bucket(bucket, ui=self.ui, dry_run=self.dry_run)
		self.client = self.bucket.client
		self.object_name = object_name
	
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
		if type(content) is str:
			if self.dry_run:
				self.ui.dry_run(f"s3.put_object('{self.bucket.name}', '{self.object_name}')")
				return True
			
			response = self.client.put_object(
				Body=content,
				Bucket=self.bucket.name,
				Key=self.object_name
			)
		else:
			if self.dry_run:
				self.ui.dry_run(f"s3.upload_fileobj('{self.bucket.name}', '{self.object_name}')")
				return True
			
			response = self.client.upload_fileobj(
				io.BytesIO(content),
				self.bucket.name,
				self.object_name
			)
		if not common.is_success(response):
			return False
		return True
	
