# print("Loaded SNS module")

import boto3
import json
from datetime import datetime
from botocore.exceptions import ClientError

sns = boto3.client('sns')


class Topic:
	"""
	import aws.sns
	topic = aws.sns.Topic(topic_arn)
	
	Requires sns:Publish
	"""
	def __init__(self, topic_arn):
		if not topic_arn:
			raise ValueError("An SNS topic name is required")
		self.topic_arn = topic_arn
	
	
	def get_ts(self):
		return datetime.utcnow().isoformat(' ')
	
	
	def publish(self, message, debug=False):
		"""
		message_id = topic.publish(message)
		"""
		if type(message) is str:
			response = sns.publish(
				TopicArn = self.topic_arn,
				Message = message,
				MessageStructure = 'string'
			)
			if debug:
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


"""
messages = get_sns_messages(event)
"""
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
