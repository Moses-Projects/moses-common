# print("Loaded SESv1 module")

import re
from boto3 import client as boto3_client

import moses_common.__init__ as common
import moses_common.ui


class Email:
	"""
	import moses_common.ses
	email = moses_common.ses.Email(from_address_arn)
	email = moses_common.ses.Email(from_address_arn, log_level=5, dry_run=False)
	"""
	def __init__(self, from_address_arn, log_level=5, dry_run=False):
		self.log_level = log_level
		self.dry_run = dry_run
		self.ui = moses_common.ui.Interface(use_slack_format=True)
	
		self.client = boto3_client('ses', region_name="us-west-2")
		self.from_address_arn = from_address_arn
		self.from_address = re.sub(r'^.*/', '', from_address_arn)
		
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	"""
	message_id = email.send({
		"to": str or list or None,
		"cc": str or list or None,
		"bcc": str or list or None,
		"reply_to": str or list or None,
		
		"charset": None (7-bit ASCII) or 'UTF-8' or 'ISO-8859-1' or 'Shift_JIS'
		"subject": str,
		"text": text_of_body,
		"html": html_of_body
	})
	"""
	def send(self, args):
		to_addresses = common.to_list(args.get('to'))
		cc_addresses = common.to_list(args.get('cc'))
		bcc_addresses = common.to_list(args.get('bcc'))
		
		content = {}
		if args.get('raw'):
			args['raw'] = re.sub(r'Return-Path:.*?\r\n', r'Return-Path: <{}>\r\n'.format(self.from_address), args['raw'], 1, flags=re.IGNORECASE)
			args['raw'] = re.sub(r'\r\nFrom:.*?\r\n', r'\r\nFrom: <{}>\r\n'.format(self.from_address), args['raw'], 1, flags=re.IGNORECASE)
			content = {
				"Data": bytes(args['raw'], 'utf-8')
			}
		
		tags_list = []
		if 'tags' in args:
			tags_list = common.convert_tags(args['tags'], 'upper')
		
		if self.log_level >= 7:
			if to_addresses:
				self.ui.body(f"To: {to_addresses}")
			if cc_addresses:
				self.ui.body(f"CC: {cc_addresses}")
			if bcc_addresses:
				self.ui.body(f"BCC: {bcc_addresses}")
			self.ui.body(f"Body: {content}")
		
		if self.dry_run:
			self.ui.dry_run(f"ses.send()")
			return 'MessageId-*'
		
		print("content {}: {}".format(type(content), content))
		response = self.client.send_raw_email(
			Source = self.from_address,
			Destinations = to_addresses,
			RawMessage = content,
			FromArn = self.from_address_arn,
			SourceArn = self.from_address_arn,
			ReturnPathArn = self.from_address_arn,
			Tags = tags_list
		)
		if type(response) is dict and 'MessageId' in response:
			return response['MessageId']
		return None
	
