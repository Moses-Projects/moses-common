# print("Loaded CloudWatch Logs module")

import os
import re
import time
from datetime import date
from botocore.exceptions import ClientError
from boto3 import client as boto3_client

import moses_common.__init__ as common
import moses_common.ui



"""
import moses_common.cloudwatch_logs
"""

class LogGroup:
	"""
	log_group = moses_common.cloudwatch_logs.LogGroup(log_group_name_prefix)
	"""
	def __init__(self, log_group_name_prefix):
		self.client = boto3_client('logs', region_name="us-west-2")
		self.name = log_group_name_prefix
		self.info = self.load()
		if self.info and type(self.info) is dict:
			self.exists = True
		else:
			self.exists = False
	
	"""
	log_group_info = log_group.load()
	"""
	def load(self):
		response = {}
		try:
			response = self.client.describe_log_groups(
				logGroupNamePrefix = self.name
			)
		except ClientError as e:
			if e.response['Error']['Code'] == 'LogGroupNotFound':
				return False
			raise e
		
		if 'logGroups' in response and type(response['logGroups']) is list and len(response['logGroups']):
			return response['logGroups'][0]
		return None
	
	@property
	def arn(self):
		if not self.exists:
			return None
		return re.sub(r':\*$', '', self.info.get('arn'))
	
	"""
	tags = log_group.load_tags()
	"""
	def load_tags(self):
		response = self.client.list_tags_log_group(
			logGroupName = self.name
		)
		
		if 'tags' in response and type(response['tags']) is dict:
			return response['tags']
		return False
	
	"""
	tags = log_group.get_tags()
	"""
	def get_tags(self):
		if 'tags' not in self.info:
			self.info['tags'] = self.load_tags()
		return self.info['tags']
	
	"""
	log_group_arn = log_group.create(args)
	log_group_arn = log_group.create({
		'tags': {               # optional
			'name1': "value1"
		},
		'retention_days': 14    # optional; can be None for no retention policy
	})
	"""
	def create(self, args):
		if type(args) is not dict:
			raise AttributeError("create args should be a dict.")
		
		response = self.client.create_log_group(
			logGroupName = self.name,
			tags = args['tags']
		)
		
		if 'ResponseMetadata' in response and type(response['ResponseMetadata']) is dict and 'HTTPStatusCode' in response['ResponseMetadata']:
			if response['ResponseMetadata']['HTTPStatusCode'] == 200:
				self.info = self.load()
				if self.info and type(self.info) is dict:
					self.exists = True
					if 'retention_days' in args:
						self.set_retention_days(args['retention_days'])
					return self.arn
		return False
	
	"""
	log_group_arn = log_group.delete()
	"""
	def delete(self):
		response = self.client.delete_log_group(
			logGroupName = self.name
		)
# 		print("response {}: {}".format(type(response), response))
		if 'ResponseMetadata' in response and type(response['ResponseMetadata']) is dict and 'HTTPStatusCode' in response['ResponseMetadata']:
			if response['ResponseMetadata']['HTTPStatusCode'] == 200:
				self.info = False
				self.exists = False
				return True
		return False
	
	"""
	success = log_group.set_retention_days(retention_days)
	"""
	def set_retention_days(self, retention_days=None):
		response = None
		if retention_days:
			if type(retention_days) is not int and type(retention_days) is not str and type(retention_days) is not float:
				raise AttributeError("retention_days should be an integer.")
			
			response = self.client.put_retention_policy(
				logGroupName = self.name,
				retentionInDays = int(retention_days)
			)
		else:
			response = self.client.delete_retention_policy(
				logGroupName = self.name
			)
			
		if 'ResponseMetadata' in response and type(response['ResponseMetadata']) is dict and 'HTTPStatusCode' in response['ResponseMetadata']:
			if response['ResponseMetadata']['HTTPStatusCode'] == 200:
				self.info['retentionInDays'] = retention_days
				return True
		return False
	
	"""
	success = log_group.tag(tags)
	"""
	def tag(self, tags):
		if type(tags) is not dict:
			raise AttributeError("tags should be a dict.")
		
		response = self.client.tag_log_group(
			logGroupName = self.name,
			tags = tags
		)
		
		if 'ResponseMetadata' in response and type(response['ResponseMetadata']) is dict and 'HTTPStatusCode' in response['ResponseMetadata']:
			if response['ResponseMetadata']['HTTPStatusCode'] == 200:
				self.info['tags'] = tags
				return True
		return False
	
	"""
	success = log_group.untag()
	"""
	def untag(self):
		response = self.client.untag_log_group(
			logGroupName = self.name
		)
		
		if 'ResponseMetadata' in response and type(response['ResponseMetadata']) is dict and 'HTTPStatusCode' in response['ResponseMetadata']:
			if response['ResponseMetadata']['HTTPStatusCode'] == 200:
				self.info['tags'] = None
				return True
		return False
	

class LogStream:
	"""
	log = moses_common.cloudwatch_logs.LogStream(log_group_name_prefix, log_stream_name_prefix)
	
	Requires the following IAM permissions:
		logs:DescribeLogGroups
		logs:CreateLogGroup
		logs:DescribeLogStreams
		logs:CreateLogStream
		logs:PutLogEvents
	"""
	def __init__(self, log_group_name_prefix, log_stream_name_prefix):
		self.client = boto3_client('logs', region_name="us-west-2")
		if not log_group_name_prefix:
			raise ValueError("A log group name is required")
		if not log_stream_name_prefix:
			raise ValueError("A log stream name is required")
		
		aws_lambda_function_name = os.environ.get('AWS_LAMBDA_FUNCTION_NAME')
		self.environment = None
		if aws_lambda_function_name:
			if re.search(r"\-dev\-", aws_lambda_function_name):
				self.environment = 'dev'
			elif re.search(r"\-prod\-", aws_lambda_function_name):
				self.environment = 'prod'
		
		
		aws_lambda_log_stream_name = None;
		if 'AWS_LAMBDA_LOG_STREAM_NAME' in os.environ:
			aws_lambda_log_stream_name = os.environ['AWS_LAMBDA_LOG_STREAM_NAME']
		self.stream_suffix = None
		if aws_lambda_log_stream_name:
			stream_suffix = re.search(r"([a-f0-9]+)$", aws_lambda_log_stream_name)
			if stream_suffix.group():
				self.stream_suffix = stream_suffix.group()
		
		# Check log group
		self.log_group_name = log_group_name_prefix
		if self.environment:
			self.log_group_name += '_' + self.environment
		group = self.get_log_group()
		if not group:
			self.create_log_group()
		
		# Get log stream
		self.log_stream_name_prefix = log_stream_name_prefix
		self.next_token = None
		self.log_stream_name = None
		self.check_log_stream()
	
	
	def get_ts(self):
		return int(round(time.time() * 1000))
	
	
	def check_log_stream(self, debug=None):
		log_stream_name = self.log_stream_name_prefix + '_' + date.today().isoformat()
		if self.stream_suffix:
			log_stream_name += '_' + self.stream_suffix
		if log_stream_name == self.log_stream_name:
			return
		
		self.log_stream_name = log_stream_name
		streams = self.get_log_streams()
		if streams and type(streams) is list and len(streams):
			self.next_token = streams[0].get('uploadSequenceToken')
		else:
			self.create_log_stream()
	
	
	def get_log_group(self, debug=None):
		"""
		log_group = log.get_log_group()
		"""
# 		response = self._call_cloudwatch_logs("describe-log-groups --log-group-name-prefix self.log_group_name", debug)
		try:
			response = self.client.describe_log_groups(
				logGroupNamePrefix = self.log_group_name
			)
		except ClientError as e:
			raise ConnectionError("Failed to get log group in Cloudwatch Logs {}: {}".format(self.log_group_name, e))
		else:
			for group in response['logGroups']:
				if group['logGroupName'] == self.log_group_name:
					if debug:
						print('get_log_group', group)
					return group
	
	
	def create_log_group(self, debug=None):
		"""
		log.create_log_group()
		"""
		try:
			response = self.client.create_log_group(
				logGroupName = self.log_group_name
			)
		except ClientError as e:
			raise ConnectionError("Failed to create log group in Cloudwatch Logs {}".format(self.log_group_name))
	
	
	def get_log_streams(self, debug=None):
		"""
		log_streams = log.get_log_streams()
		"""
# 		response = self._call_cloudwatch_logs("describe-log-streams --log-group-name self.log_group_name --log-stream-name-prefix self.log_stream_name", debug)
		try:
			response = self.client.describe_log_streams(
				logGroupName = self.log_group_name,
				logStreamNamePrefix = self.log_stream_name,
				limit = 1
			)
		except ClientError as e:
			print("e {}: {}".format(type(e), e))
			raise ConnectionError("Failed to get log streams in Cloudwatch Logs {}/{}".format(self.log_group_name, self.log_stream_name))
		else:
			streams = []
			for stream in response['logStreams']:
				if stream['logStreamName'] == self.log_stream_name:
					streams.append(stream)
			if not len(streams):
				return
			if debug:
				print('get_log_streams', streams)
			return streams
	
	
	def create_log_stream(self, debug=None):
		"""
		log.create_log_stream()
		"""
# 		response = self._call_cloudwatch_logs("create-log-stream --log-group-name self.log_group_name --log-stream-name self.log_stream_name", debug)
		try:
			response = self.client.create_log_stream(
				logGroupName = self.log_group_name,
				logStreamName = self.log_stream_name
			)
		except ClientError as e:
			raise ConnectionError("Failed to create log stream in Cloudwatch Logs {}/{}".format(self.log_group_name, self.log_stream_name))
		else:
			self.next_token = None
	
	
	def put_log_event(self, message):
		"""
		log.put_log_event(message)
		"""
		self.check_log_stream()
		if type(message) is dict or type(message) is list:
			message = common.make_json(message)
		try:
			if self.next_token:
				response = self.client.put_log_events(
					logGroupName = self.log_group_name,
					logStreamName = self.log_stream_name,
					sequenceToken = self.next_token,
					logEvents = [
						{
							'timestamp': self.get_ts(),
							'message': message
						}
					]
				)
			else:
				response = self.client.put_log_events(
					logGroupName = self.log_group_name,
					logStreamName = self.log_stream_name,
					logEvents = [
						{
							'timestamp': self.get_ts(),
							'message': message
						}
					]
				)
		except ClientError as e:
			raise ConnectionError("Failed to put log event in Cloudwatch Logs {}/{}".format(self.log_group_name, self.log_stream_name))
		else:
			if 'nextSequenceToken' in response:
				self.next_token = response['nextSequenceToken']

