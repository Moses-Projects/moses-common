# print("Loaded SNS module")

import boto3
import json
import re
from datetime import datetime
from boto3 import client as boto3_client

import moses_common.__init__ as common
import moses_common.ui


class Topic:
	"""
	import moses_common.sns
	topic = moses_common.sns.Email(topic_arn)
	topic = moses_common.sns.Email(topic_arn, ui=None, dry_run=False)
	
	Requires sns:Publish
	"""
	def __init__(self, topic_arn, ui=None, dry_run=False):
		self.dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
	
		self.client = boto3_client('sns', region_name="us-west-2")
		
		if not topic_arn:
			raise ValueError("An SNS topic name is required")
		self.topic_arn = topic_arn
	
	
	def get_ts(self):
		return datetime.utcnow().isoformat(' ')
	
	
	def publish(self, message):
		"""
		message_id = topic.publish(message)
		"""
		if type(message) is str:
			response = sns.publish(
				TopicArn = self.topic_arn,
				Message = message,
				MessageStructure = 'string'
			)
			if self.log_level >= 6:
				print("response:", response)
		elif type(message) is dict:
			response = sns.publish(
				TopicArn = self.topic_arn,
				Subject = message.get('subject'),
				Message = message.get('message'),
				MessageStructure = 'json',
				MessageAttributes={
					'string': {
						'DataType': 'string',
						'StringValue': 'string',
						'BinaryValue': b'bytes'
					}
				}
			)
		else:
			return
		
		if type(response) is dict and 'ResponseMetadata' in response:
			if response['ResponseMetadata'].get('HTTPStatusCode') == 200:
				return response.get('MessageId')
		
# {
# 	"MessageId": "e87cf24f-f71b-53be-a6a5-a10cc3de25f9",
# 	"ResponseMetadata": {
# 		"HTTPHeaders": {
# 			"content-length": "294",
# 			"content-type": "text/xml",
# 			"date": "Sat, 07 Apr 2018 21:05:09 GMT",
# 			"x-amzn-requestid": "550f44e9-f685-556d-896d-17aac240c32a"
# 		},
# 		"HTTPStatusCode": "200",
# 		"RequestId": "550f44e9-f685-556d-896d-17aac240c32a",
# 		"RetryAttempts": "0"
# 	}
# }


class Notification:
	"""
	import moses_common.sns
	notification = moses_common.sns.Notification(event)
	notification = moses_common.sns.Notification(event, ui=None, dry_run=False)
	"""
	def __init__(self, event, ui=None, dry_run=False):
		self.dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
		
		self.records = self.get_records(event)
		self.messages = self.get_messages()
	
	def get_records(self, event):
		if type(event) is not dict or "Records" not in event or type(event['Records']) is not list or not event['Records']:
			return []
		
		records = []
		for record in event['Records']:
			if type(record) is not dict or "EventSource" not in record or record["EventSource"] != "aws:sns" or "Sns" not in record:
				continue
			records.append(record["Sns"])
		return records
	
	def get_messages(self):
		messages = []
		for record in self.records:
			if "Message" not in record:
				continue
			
			message_data = re.sub(r'\\\\([\(\)])', r'$1', record["Message"])
			message_data = common.convert_value(message_data)
			message = moses_common.sns.Message(message_data, ui=self.ui, dry_run=self.dry_run)
			messages.append(message)
		return messages



class Message:
	"""
	import moses_common.sns
	message = moses_common.sns.Message(raw_message_data)
	message = moses_common.sns.Message(raw_message_data, ui=None, dry_run=False)
	"""
	def __init__(self, data, ui=None, dry_run=False):
		self.dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
		
		self.ui.debug("data {}: {}".format(type(data), data))
		self.data = data
		
	@property
	def mail_headers(self):
		if 'mail' in self.data and 'commonHeaders' in self.data['mail']:
			return self.data['mail']['commonHeaders']
		return {}
	
	@property
	def mail_recipients(self):
		if 'receipt' in self.data and 'recipients' in self.data['receipt']:
			return self.data['receipt']['recipients']
		return {}
	
	@property
	def mail_subject(self):
		headers = self.mail_headers
		if 'subject' in headers:
			return headers['subject']
		return None
	
	@property
	def mail_from(self):
		headers = self.mail_headers
		if 'from' in headers:
			return headers['from']
		return None
	
	@property
	def mail_date(self):
		headers = self.mail_headers
		if 'date' in headers:
			return headers['date']
		return None
	
	@property
	def mail_to(self):
		headers = self.mail_headers
		if 'to' in headers:
			return headers['to']
		return None
	
	@property
	def mail_cc(self):
		headers = self.mail_headers
		if 'cc' in headers:
			return headers['cc']
		return None
	
	@property
	def mail_content(self):
		if 'content' in self.data:
			return common.convert_value(self.data['content'])
		return None
	
