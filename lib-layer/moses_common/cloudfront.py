# print("Loaded CloudFront module")

import json
import re
from botocore.exceptions import ClientError
from boto3 import client as boto3_client

import moses_common.__init__ as common
import moses_common.ui


class Distribution:
	"""
	import moses_common.cloudfront
	distribution = moses_common.cloudfront.Distribution(hostname, ui=ui, dry_run=dry_run)
	"""
	def __init__(self, hostname, ui=None, dry_run=False):
		self.dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
		self.client = boto3_client('cloudfront', region_name="us-west-2")
		
		self.hostname = hostname
		self.policies = {}
		self.info = None
		
		if self.load():
			self.exists = True
		else:
			self.exists = False
	
	"""
	distribution_info = distribution.load()
	"""
	def load(self):
		try:
			response = self.client.list_distributions()
		except ClientError as e:
			raise e
		else:
# 			print("response {}: {}".format(type(response), response))
			if common.is_success(response) and 'DistributionList' in response and 'Items' in response['DistributionList'] and type(response['DistributionList']['Items']) is list:
				for item in response['DistributionList']['Items']:
					if 'Aliases' in item and 'Items' in item['Aliases'] and type(item['Aliases']['Items']) is list:
						for hn in item['Aliases']['Items']:
							if hn == self.hostname:
								self.info = item
								return True
			return False
	
	"""
	distribution_config, etag = distribution.get_config()
	"""
	def get_config(self):
		try:
			response = self.client.get_distribution_config(
				Id = self.id
			)
		except ClientError as e:
			raise e
		else:
# 			print("response {}: {}".format(type(response), response))
			if common.is_success(response) and 'DistributionConfig' in response:
				return response['DistributionConfig'], response['ETag']
			return None, None
	
	def load_cache_policies(self):
		if 'cache' in self.policies and len(self.policies['cache']):
			return True
		try:
			response = self.client.list_cache_policies()
		except ClientError as e:
			raise e
		else:
# 			print("response {}: {}".format(type(response), response))
			self.policies['cache'] = {}
			if common.is_success(response) and 'CachePolicyList' in response and 'Items' in response['CachePolicyList'] and type(response['CachePolicyList']['Items']) is list:
				for policy in response['CachePolicyList']['Items']:
					if 'CachePolicy' in policy and 'CachePolicyConfig' in policy['CachePolicy']:
						policy_name = re.sub(r'^(Managed|Custom)-', '', policy['CachePolicy']['CachePolicyConfig']['Name'])
						self.policies['cache'][policy_name] = policy['CachePolicy']['Id']
				return True
			return False
	
	def load_origin_request_policies(self):
		if 'origin_request' in self.policies and len(self.policies['origin_request']):
			return True
		try:
			response = self.client.list_origin_request_policies()
		except ClientError as e:
			raise e
		else:
# 			print("response {}: {}".format(type(response), response))
			self.policies['origin_request'] = {}
			if common.is_success(response) and 'OriginRequestPolicyList' in response and 'Items' in response['OriginRequestPolicyList'] and type(response['OriginRequestPolicyList']['Items']) is list:
				for policy in response['OriginRequestPolicyList']['Items']:
					if 'OriginRequestPolicy' in policy and 'OriginRequestPolicyConfig' in policy['OriginRequestPolicy']:
						policy_name = re.sub(r'^(Managed|Custom)-', '', policy['OriginRequestPolicy']['OriginRequestPolicyConfig']['Name'])
						self.policies['origin_request'][policy_name] = policy['OriginRequestPolicy']['Id']
				return True
			return False
	
	def load_response_headers_policies(self):
		if 'response_headers' in self.policies and len(self.policies['response_headers']):
			return True
		try:
			response = self.client.list_response_headers_policies()
		except ClientError as e:
			raise e
		else:
# 			print("response {}: {}".format(type(response), response))
			self.policies['response_headers'] = {}
			if common.is_success(response) and 'ResponseHeadersPolicyList' in response and 'Items' in response['ResponseHeadersPolicyList'] and type(response['ResponseHeadersPolicyList']['Items']) is list:
				for policy in response['ResponseHeadersPolicyList']['Items']:
					if 'ResponseHeadersPolicy' in policy and 'ResponseHeadersPolicyConfig' in policy['ResponseHeadersPolicy']:
						policy_name = re.sub(r'^(Managed|Custom)-', '', policy['ResponseHeadersPolicy']['ResponseHeadersPolicyConfig']['Name'])
						self.policies['response_headers'][policy_name] = policy['ResponseHeadersPolicy']['Id']
				return True
			return False
	

	@property
	def id(self):
		if not self.exists or 'Id' not in self.info:
			return None
		return self.info['Id']
	
	@property
	def arn(self):
		if not self.exists or 'ARN' not in self.info:
			return None
		return self.info['ARN']
	
	@property
	def enabled(self):
		if not self.exists or 'Enabled' not in self.info:
			return None
		return self.info['Enabled']
	
	@property
	def cloudfront_hostname(self):
		if not self.exists or 'DomainName' not in self.info:
			return None
		return self.info['DomainName']
	
	def get_origins(self):
		if not self.exists or 'Origins' not in self.info:
			return None
		if 'Items' not in self.info['Origins']:
			return None
		return self.info['Origins']['Items']
	
	def get_origin(self, name):
		origins = self.get_origins()
		if not origins:
			return None
		for origin in origins:
			if origin['Id'] == name:
				return origin
		return None
	
	def get_behaviors(self):
		if not self.exists or 'CacheBehaviors' not in self.info:
			return None
		if 'Items' not in self.info['CacheBehaviors']:
			return None
		return self.info['CacheBehaviors']['Items']
	
	def get_behavior(self, path_pattern):
		behaviors = self.get_behaviors()
		if not behaviors:
			return None
		for behavior in behaviors:
			if path_pattern == behavior['PathPattern']:
				return behavior
		return None
	
	def get_default_behavior(self):
		if not self.exists or 'DefaultCacheBehavior' not in self.info:
			return None
		return self.info['DefaultCacheBehavior']
	
	"""
	success = distribution.invalidate()
	success = distribution.invalidate("/assets/*")
	"""
	def invalidate(self, path=None):
		reference = common.normalize(path)
		if not path:
			path = '/*'
			reference = 'entire_site'
		
		if self.dry_run:
			self.ui.dry_run(f"create_invalidation('{path}')")
			return True
		
		response = self.client.create_invalidation(
			DistributionId = self.id,
			InvalidationBatch = {
				'Paths': {
					'Quantity': 1,
					'Items': [ path ]
				},
				'CallerReference': reference
			}
		)
		if common.is_success(response) and 'Invalidation' in response:
			return True
		return False
	
	
	"""
	origin = distribution.generate_origin_config(id, hostname, args)
	"""
	def generate_origin_config(self, id, hostname, args={}):
		origin_type = 'elb'
		if re.search(r'\.s3\.', hostname):
			origin_type = 's3'
		
		path = ""
		if 'path' in args:
			path = args['path']
		
		custom_headers = { "Quantity": 0 }
		if 'custom_headers' in args:
			custom_headers = {
				"Quantity": len(args['custom_headers']),
				"Items": []
			}
			if type(args['custom_headers']) is dict:
				for key, value in args['custom_headers'].items():
					custom_headers['Items'].append({
						"HeaderName": key,
						"HeaderValue": value
					})
		
		origin = {
			"Id": id,
			"DomainName": hostname,
			"OriginPath": path,
			"CustomHeaders": custom_headers,
			"ConnectionAttempts": 3,
			"ConnectionTimeout": 10,
			"OriginShield": {
				"Enabled": False
			}
		}
		if re.search(r'\.s3\.', hostname) and 'origin_access_identity' in args:
			origin['S3OriginConfig'] = {
				"OriginAccessIdentity": args['origin_access_identity']
			}
		else:
			origin['CustomOriginConfig'] = {
				"HTTPPort": 80,
				"HTTPSPort": 443,
				"OriginProtocolPolicy": "https-only",
				"OriginSslProtocols": {
					"Quantity": 1,
					"Items": ["TLSv1.2"]
				},
				"OriginReadTimeout": 30,
				"OriginKeepaliveTimeout": 5
			}
		return origin
	
	"""
	behavior = distribution.generate_behavior_config(path_pattern, origin_id, args)
	"""
	def generate_behavior_config(self, path_pattern, origin_id, args):
		allowed_methods = ["GET", "HEAD", "OPTIONS"]
		if 'allowed_methods' in args and args['allowed_methods'] == 'POST':
			allowed_methods = ["GET", "HEAD", "POST", "PUT", "PATCH", "OPTIONS", "DELETE"]
		
		
		lambda_function_associations = { "Quantity": 0 }
		if 'lambda_function_associations' in args:
			lambda_function_associations = {
				"Quantity": len(args['lambda_function_associations']),
				"Items": args['lambda_function_associations']
			}
		
		self.load_cache_policies()
# 		print("self.policies['cache'] {}: {}".format(type(self.policies['cache']), self.policies['cache']))
		cache_policy_id = self.policies['cache']['CachingDisabled']
		if 'cache_policy' in args:
			cache_policy_id = self.policies['cache'][args['cache_policy']]
		
		self.load_origin_request_policies()
# 		print("self.policies['origin_request'] {}: {}".format(type(self.policies['origin_request']), self.policies['origin_request']))
		origin_request_policy_id = ""
		if 'origin_request_policy' in args:
			origin_request_policy_id = self.policies['origin_request'][args['origin_request_policy']]
		
		self.load_response_headers_policies()
# 		print("self.policies['response_headers'] {}: {}".format(type(self.policies['response_headers']), self.policies['response_headers']))
		response_headers_policy_id = ""
		if 'response_headers_policy' in args:
			response_headers_policy_id = self.policies['response_headers'][args['response_headers_policy']]
		
		behavior = {
			"TargetOriginId": origin_id,
			"TrustedSigners": {
				"Enabled": False,
				"Quantity": 0
			},
			"TrustedKeyGroups": {
				"Enabled": False,
				"Quantity": 0
			},
			"ViewerProtocolPolicy": "redirect-to-https",
			"FieldLevelEncryptionId": "",
			"AllowedMethods": {
				"Quantity": len(allowed_methods),
				"Items": allowed_methods,
				"CachedMethods": {
					"Quantity": 2,
					"Items": ["GET", "HEAD"]
				}
			},
			"SmoothStreaming": False,
			"Compress": True,
			"LambdaFunctionAssociations": lambda_function_associations,
			"FunctionAssociations": {
				"Quantity": 0
			},
			"CachePolicyId": cache_policy_id,
			"OriginRequestPolicyId": origin_request_policy_id,
			"ResponseHeadersPolicyId": response_headers_policy_id
		}
		if path_pattern != '*':
			behavior['PathPattern'] = path_pattern
		return behavior
	
	
	"""
	success = distribution.update_origin(origin)
	"""
	def update_origin(self, new_origin):
		config, etag = self.get_config()
		
		origin_index = None
		origin_quantity = len(config['Origins']['Items'])
		found = False
		for i in range(origin_quantity):
			if new_origin['Id'] == config['Origins']['Items'][i]['Id']:
				origin_index = i
				found = True
				break
		if found:
			self.ui.notice(f"update origin replace at index {origin_index}")
			config['Origins']['Items'][origin_index] = new_origin
		else:
			self.ui.notice("update origin append")
			config['Origins']['Items'].append(new_origin)
		
		config['Origins']['Quantity'] = len(config['Origins']['Items'])
		
		if self.dry_run:
			self.ui.dry_run(f"update origin ({self.id}, {config['Origins']['Items']})")
			return True
		
		response = self.client.update_distribution(
			Id = self.id,
			DistributionConfig = config,
			IfMatch = etag
		)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'Distribution' in response:
			self.info = response['Distribution']
			self.exists = True
			return True
		return False
		
	
	"""
	success = distribution.add_behavior(behavior, position)
	"""
	def add_behavior(self, behavior, position=100):
		config, etag = self.get_config()
		
		config['CacheBehaviors']['Items'].insert(position, behavior)
		config['CacheBehaviors']['Quantity'] = len(config['CacheBehaviors']['Items'])
		
		if self.dry_run:
			self.ui.dry_run(f"add behavior ({self.id}, {config})")
			return True
		
		response = self.client.update_distribution(
			Id = self.id,
			DistributionConfig = config,
			IfMatch = etag
		)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'Distribution' in response:
			self.info = response['Distribution']
			self.exists = True
			return True
		return False
		
	
	"""
	success = distribution.remove_origin(origin_id)
	"""
	def remove_origin(self, origin_id):
		config, etag = self.get_config()
		is_changed = False
		
		behaviors_to_remove = []
		for behavior in config['CacheBehaviors']['Items']:
			if behavior['TargetOriginId'] == origin_id:
				behaviors_to_remove.append(behavior)
		for behavior in behaviors_to_remove:
			config['CacheBehaviors']['Items'].remove(behavior)
			is_changed = True
		config['CacheBehaviors']['Quantity'] = len(config['CacheBehaviors']['Items'])
		
		for i in range(len(config['Origins']['Items'])):
			if config['Origins']['Items'][i]['Id'] == origin_id:
				config['Origins']['Items'].pop(i)
				is_changed = True
				break
		config['Origins']['Quantity'] = len(config['Origins']['Items'])
		if not is_changed:
			return True
		
		if self.dry_run:
			self.ui.dry_run(f"remove origin ({self.id}, {config})")
			return True
		
		response = self.client.update_distribution(
			Id = self.id,
			DistributionConfig = config,
			IfMatch = etag
		)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'Distribution' in response:
			self.info = response['Distribution']
			self.exists = True
			return True
		return False
		
	
	"""
	etag = distribution.disable()
	"""
	def disable(self):
		config, etag = self.get_config()
		
		config['Enabled'] = False
		
		if self.dry_run:
			self.ui.dry_run(f"disable distribution ({self.id}, {config})")
			return True
		
		response = self.client.update_distribution(
			Id = self.id,
			DistributionConfig = config,
			IfMatch = etag
		)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'Distribution' in response:
			self.info = response['Distribution']
			self.exists = True
			return response['ETag']
		return False
		
	
	"""
	etag = distribution.update(config, etag)
	"""
	def update(self, config, etag):
		if self.dry_run:
			self.ui.dry_run(f"update ({self.id}, {config})")
			return True
		
		response = self.client.update_distribution(
			Id = self.id,
			DistributionConfig = config,
			IfMatch = etag
		)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'Distribution' in response:
			self.info = response['Distribution']
			self.exists = True
			return response['ETag']
		return False
		
	
	"""
	success = distribution.create(args)
	"""
	def create(self, args):
		if not args or type(args) is not dict:
			raise AttributeError("Received invalid args.")
		
		origins = []
		if 'origins' in args:
			origins = args['origins']
		
		default_behavior = []
		if 'default_behavior' in args:
			default_behavior = args['default_behavior']
		
		behaviors = []
		if 'behaviors' in args:
			behaviors = args['behaviors']
		
		error_responses = []
		if 'errors' in args:
			error_responses = args['errors']
			for error in error_responses:
				if 'ErrorCachingMinTTL' not in error:
					error['ErrorCachingMinTTL'] = 10
				error['ErrorCode'] = int(error['ErrorCode'])
				error['ResponseCode'] = str(error['ResponseCode'])
		
		comment = ""
		if 'description' in args:
			comment = args['description']
		
		logging = {
			"Enabled": False
		}
		if 'logging' in args:
			logging = {
				"Enabled": True,
				"IncludeCookies": True,
				"Bucket": args['logging']['bucket'],
				"Prefix": args['logging']['prefix']
			}		
		
		certificate = {
			"CloudFrontDefaultCertificate": True
		}
		if 'certificate' in args:
			certificate = args['certificate']	
		
		config = {
			"CallerReference": self.hostname,
			"Aliases": {
				"Quantity": 1,
				"Items": [self.hostname]
			},
			"DefaultRootObject": "index.html",
			"Origins": {
				"Quantity": len(origins),
				"Items": origins
			},
			"OriginGroups": {
				"Quantity": 0
			},
			"DefaultCacheBehavior": default_behavior,
			"CacheBehaviors": {
				"Quantity": len(behaviors),
				"Items": behaviors
			},
			"CustomErrorResponses": {
				"Quantity": len(error_responses),
				"Items": error_responses
			},
			"Comment": comment,
			"Logging": logging,
			"PriceClass": "PriceClass_100",
			"Enabled": True,
			"ViewerCertificate": certificate,
			"Restrictions": {
				"GeoRestriction": {
					"RestrictionType": "none",
					"Quantity": 0
				}
			},
			"WebACLId": "",
			"HttpVersion": "http2",
			"IsIPV6Enabled": True
		}
		
		print("config {}: {}".format(type(config), config))
		
		response = {}
		if 'tags' in args:
			tags_list = common.convert_tags(args['tags'], 'upper')
			
			if self.dry_run:
				self.ui.dry_run(f"create with tags ({config}, {tags_list})")
				return True
		
			response = self.client.create_distribution_with_tags(
				DistributionConfigWithTags = {
					"DistributionConfig": config,
					"Tags": {
						"Items": tags_list
					}
				}
			)
		else:
			if self.dry_run:
				self.ui.dry_run(f"create ({config})")
				return True
		
			response = self.client.create_distribution(
				DistributionConfig = config
			)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'Distribution' in response:
			self.info = response['Distribution']
			self.exists = True
			return True
		return False
		
	
	"""
	success = distribution.delete(etag)
	"""
	def delete(self, etag):
		if self.dry_run:
			self.ui.dry_run(f"delete ({self.id}, {config})")
			return True
		
		response = self.client.delete_distribution(
			Id = self.id,
			IfMatch = etag
		)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response):
			self.info = None
			self.exists = False
			return True
		return False
		
