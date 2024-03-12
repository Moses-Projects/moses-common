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
	topic = moses_common.sns.Email(topic_arn, log_level=5, dry_run=False)
	
	Requires sns:Publish
	"""
	def __init__(self, topic_arn, log_level=5, dry_run=False):
		self.log_level = log_level
		self.dry_run = dry_run
		self.ui = moses_common.ui.Interface(use_slack_format=True)
	
		self.client = boto3_client('sns', region_name="us-west-2")
		
		if not topic_arn:
			raise ValueError("An SNS topic name is required")
		self.topic_arn = topic_arn
	
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
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
	notification = moses_common.sns.Notification(event, log_level=5, dry_run=False)
	"""
	def __init__(self, event, log_level=5, dry_run=False):
		self.log_level = log_level
		self.dry_run = dry_run
		self.ui = moses_common.ui.Interface(use_slack_format=True)
		
		self.records = self.get_records(event)
		self.messages = self.get_messages()
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
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
			message = moses_common.sns.Message(message_data, log_level=self.log_level, dry_run=self.dry_run)
			messages.append(message)
		return messages



class Message:
	"""
	import moses_common.sns
	message = moses_common.sns.Message(raw_message_data)
	message = moses_common.sns.Message(raw_message_data, log_level=5, dry_run=False)
	"""
	def __init__(self, data, log_level=5, dry_run=False):
		self.log_level = log_level
		self.dry_run = dry_run
		self.ui = moses_common.ui.Interface(use_slack_format=True)
		
		self.data = data
		
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def mail_headers(self):
		if 'mail' in self.data and 'commonHeaders' in self.data['mail']:
			return self.data['mail']['commonHeaders']
		return None
	
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
	
	
	
	def convert_for_ses(self):
		message = self.data
		if 'content' not in message or 'mail' not in message:
			return None
		if 'headers' not in message['mail']:
			return None
		email = {
			"charset": None,
			"subject": ""
		}
		
		# Get MIME boundary
		boundary = None
		if 'headers' in message['mail']:
			mime_version = None
			content_type = None
			for header in message['mail']['headers']:
				if header['name'].lower() == 'mime-version':
					mime_version = header['value']
				elif header['name'].lower() == 'content-type':
					content_type = header['value']
				elif header['name'].lower() == 'subject':
					email['subject'] = header['value']
			if mime_version and re.match(r'multipart/alternative', content_type):
				parts = re.search(r'boundary=(.*)', content_type)
				if parts and parts[1]:
					boundary = re.sub(r"'", '', parts[1])
					if self.log_level >= 7:
						print("boundary {}: {}".format(type(boundary), boundary))
		
		body = common.convert_value(message['content'])
		
		lines = re.split(r'\r\n', body)
		in_body = False
		in_part = False
		in_part_body = False
		part_type = None
		b_line = re.compile("--{}".format(boundary))
		b_end = re.compile("--{}--".format(boundary))
		for line in lines:
			if self.log_level >= 7:
				print(f"# {line}")
			# Skip past headers
			if not in_body:
				if not len(line):
					in_body = True
				continue
			
			if boundary:
				if self.log_level >= 7:
					print("    body")
				if b_end.match(line):
					if self.log_level >= 7:
						print("      boundary end")
					break
				if b_line.match(line):
					if self.log_level >= 7:
						print("      boundary")
					part_type = None
					in_part = True
					in_part_body = False
					continue
				if not in_part:
					if self.log_level >= 7:
						print("      outside part")
					continue
				if self.log_level >= 7:
					print("      inside part")
				if not in_part_body:
					if self.log_level >= 7:
						print("        outside part body")
					if not len(line):
						if self.log_level >= 7:
							print("          end of part header")
						in_part_body = True
						continue
					if re.match('Content-Type', line, re.IGNORECASE):
						if re.search('text/plain', line, re.IGNORECASE):
							part_type = 'text'
						elif re.search('text/html', line, re.IGNORECASE):
							part_type = 'html'
						if self.log_level >= 7:
							print(f"      part type {part_type}")
					if re.search(r'charset', line, re.IGNORECASE):
						if re.search(r'utf-?8', line, re.IGNORECASE):
							email['charset'] = 'UTF-8'
						elif re.search(r'iso-8859-1', line, re.IGNORECASE):
							email['charset'] = 'ISO-8859-1'
						elif re.search(r'shift.jis', line, re.IGNORECASE):
							email['charset'] = 'Shift_JIS'
						if self.log_level >= 7:
							print(f"      charset {email['charset']}")
						continue
				if in_part_body and part_type:
					if self.log_level >= 7:
						print(f"      {part_type} - capture")
					if part_type not in email:
						email[part_type] = ''
					email[part_type] += line + "\n"
			
			else:
				if self.log_level >= 7:
					print("      text")
				if 'text' not in email:
					email['text'] = ''
				email['text'] += line + "\n"
		
		return email

