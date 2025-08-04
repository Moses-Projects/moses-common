# print("Loaded SESv2 module")

import re
from boto3 import client as boto3_client

import moses_common.__init__ as common
import moses_common.ui


class Email:
	"""
	import moses_common.ses
	email = moses_common.ses.Email(from_address_arn)
	email = moses_common.ses.Email(from_address_arn, ui=ui, dry_run=dry_run)
	"""
	def __init__(self, from_address_arn, ui=None, dry_run=False):
		self.dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
	
		self.client = boto3_client('sesv2', region_name="us-west-2")
		self.from_address_arn = from_address_arn
		self.from_address = re.sub(r'^.*/', '', from_address_arn)
		
	
	"""
	message_id = email.send({
		"to": str or list or None,
		"cc": str or list or None,
		"bcc": str or list or None,
		"reply_to": str or list or None,
		
		"charset": None (7-bit ASCII) or 'UTF-8' or 'ISO-8859-1' or 'Shift_JIS'
		"subject": str,
		"text": text_of_body,
		"html": html_of_body,
		"raw": raw_body
	})
	"""
	def send(self, args):
		to_addresses = common.to_list(args.get('to'))
		cc_addresses = common.to_list(args.get('cc'))
		bcc_addresses = common.to_list(args.get('bcc'))
		reply_to_addresses = common.to_list(args.get('reply_to'))
		
		charset = None
		if charset in ['UTF-8', 'ISO-8859-1', 'Shift_JIS']:
			charset = args.get('charset')
		
		subject = {
			"Data": args.get('subject', '')
		}
		if charset:
			subject['Charset'] = charset
		
		content = {}
		body = {}
		if args.get('raw'):
# 			args['raw'] = re.sub(r'Return-Path:.*?\r\n', r'Return-Path: <{}>\r\n'.format(self.from_address), args['raw'], 1, flags=re.IGNORECASE)
			content = {
				"Raw": {
					"Data": bytes(args['raw'], 'utf-8')
				}
			}
		else:
			if args.get('html'):
				body['Html'] = {
					"Data": args.get('html')
				}
				if charset:
					body['Html']['Charset'] = charset
			if args.get('text'):
				body['Text'] = {
					"Data": args.get('text')
				}
				if charset:
					body['Text']['Charset'] = charset
			content = {
				"Simple": {
					"Subject": subject,
					"Body": body
				}
			}
		
		
		tags_list = []
		if 'tags' in args:
			tags_list = common.convert_tags(args['tags'], 'upper')
		
		if to_addresses:
			self.ui.debug(f"To: {to_addresses}")
		if cc_addresses:
			self.ui.debug(f"CC: {cc_addresses}")
		if bcc_addresses:
			self.ui.debug(f"BCC: {bcc_addresses}")
		if reply_to_addresses:
			self.ui.debug(f"Reply-to: {reply_to_addresses}")
		self.ui.debug(f"Subject: {subject}")
		self.ui.debug(f"Body: {body}")
		
		if self.dry_run:
			self.ui.dry_run(f"ses.send()")
			return 'MessageId-*'
		
		from_email_address = self.from_address
		if args.get('from_name'):
			from_email_address = f"{args['from_name']} <{self.from_address}>"
		
		self.ui.body("content {}: {}".format(type(content), content))
		response = self.client.send_email(
			FromEmailAddress = from_email_address,
			FromEmailAddressIdentityArn = self.from_address_arn,
			Destination = {
				"ToAddresses": to_addresses,
				"CcAddresses": cc_addresses,
				"BccAddresses": bcc_addresses
			},
			ReplyToAddresses = reply_to_addresses,
			Content = content,
			EmailTags = tags_list
		)
		if type(response) is dict and 'MessageId' in response:
			return response['MessageId']
		return None
	
