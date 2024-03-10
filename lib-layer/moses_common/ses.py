# print("Loaded SES module")

import re
from boto3 import client as boto3_client

import moses_common.__init__ as common
import moses_common.ui


class Email:
	"""
	import moses_common.ses
	email = moses_common.ses.Email(from_address_arn)
	"""
	def __init__(self, from_address_arn, log_level=5, dry_run=False):
		self.log_level = log_level
		self.dry_run = dry_run
		self.ui = moses_common.ui.Interface(use_slack_format=True)
	
		self.client = boto3_client('sesv2', region_name="us-west-2")
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
		reply_to_addresses = common.to_list(args.get('reply_to'))
		
		charset = None
		if charset in ['UTF-8', 'ISO-8859-1', 'Shift_JIS']:
			charset = args.get('charset')
		
		subject = {
			"Data": args.get('subject', '')
		}
		if charset:
			subject['Charset'] = charset
		
		body = {}
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
			if reply_to_addresses:
				self.ui.body(f"Reply-to: {reply_to_addresses}")
			self.ui.body(f"Subject: {subject}")
			self.ui.body(f"Body: {body}")
		
		if self.dry_run:
			self.ui.dry_run(f"ses.send()")
			return 'MessageId-*'
		
		
		response = self.client.send_email(
			FromEmailAddress = self.from_address,
			FromEmailAddressIdentityArn = self.from_address_arn,
			Destination = {
				"ToAddresses": to_addresses,
				"CcAddresses": cc_addresses,
				"BccAddresses": bcc_addresses
			},
			ReplyToAddresses = reply_to_addresses,
			Content = {
				"Simple": {
					"Subject": subject,
					"Body": body
				}
			},
			EmailTags = tags_list
		)
		if type(response) is dict and 'MessageId' in response:
			return response['MessageId']
		return None
	

# def handler(event, context):
# # 	print("Received event: " + json.dumps(event, indent=2))
# 	messages = get_sns_messages(event)
# 	for message in messages:
# 		bounces = get_bounces(message)
# 		found = False
# 		if type(bounces) is list:
# 			for bounce in bounces:
# 				bounce["district"] = find_district(bounce["message"])
# 				if not bounce["district"]:
# 					bounce["district"] = find_district(bounce["email_address"])
# 				session = {}
# 				session['bounce'] = bounce
# 				session['key'] = dd.get_ts()
# 				dd.put_item(session)
# 				found = True
# 		if not found:
# 			session = {}
# 			session['message'] = message
# 			session['key'] = dd.get_ts()
# 			dd.put_item(session)
# 	return "Completed bounce handling."
# 
# 
# def get_sns_messages(event):
# 	if type(event) is not dict or "Records" not in event or type(event['Records']) is not list or len(event['Records']) < 1:
# 		raise AttributeError("handler event requires a Record")
# 	messages = []
# 	for record in event['Records']:
# 		if type(record) is not dict or "EventSource" not in record or record["EventSource"] != "aws:sns" or "Sns" not in record or type(record["Sns"]) is not dict or "Message" not in record["Sns"]:
# 			continue
# 		if re.match("(\[|\{)", record["Sns"]["Message"]):
# 			# json
# 			message = json.loads(record["Sns"]["Message"])
# 			messages.append(message)
# 	
# 	if len(messages):
# 		return messages
# 	raise ValueError("No valid SNS message found")
# 
# 
# def get_bounces(message):
# 	if "bounce" not in message or type(message["bounce"]) is not dict or "bouncedRecipients" not in message["bounce"] or type(message["bounce"]["bouncedRecipients"]) is not list:
# 		return
# 	bounces = []
# 	for recip in message["bounce"]["bouncedRecipients"]:
# 		if "diagnosticCode" in recip and "emailAddress" in recip:
# 			bounce = {
# 				"message": recip["diagnosticCode"],
# 				"email_address": recip["emailAddress"]
# 			}
# 			bounces.append(bounce)
# 	if len(bounces):
# 		return bounces
# 	return