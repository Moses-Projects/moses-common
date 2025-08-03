# print("Loaded Route53 Service module")

import json
import re
from botocore.exceptions import ClientError
from boto3 import client as boto3_client

import moses_common.__init__ as common
import moses_common.ui


class Zone:
	"""
	import moses_common.route53
	zone = moses_common.route53.Zone(domain_name, ui=None, dry_run=False)
	"""
	def __init__(self, domain_name, ui=None, dry_run=False):
		self.dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
		self.client = boto3_client('route53', region_name="us-west-2")
		
		self.name = domain_name
		self.info = self.load()
		if self.info and type(self.info) is dict:
			self.exists = True
		else:
			self.exists = False
		
	"""
	zone_info = zone.load()
	"""
	def load(self):
		response = self.client.list_hosted_zones_by_name(
			DNSName = self.name,
			MaxItems = '1'
		)
# 		print("response {}: {}".format(type(response), response))
		if 'HostedZones' in response and type(response['HostedZones']) is list and len(response['HostedZones']):
			return response['HostedZones'][0]
		return False
	
	@property
	def id(self):
		if not self.exists:
			return None
		return self.info.get('Id')
	
	@property
	def short_id(self):
		if not self.exists:
			return False
		short_id = re.sub(r'/hostedzone/', '', self.id)
		return short_id
	

class Record:
	"""
	record = moses_common.route53.Record(zone, subdomain, ui=ui, dry_run=dry_run)
	record = moses_common.route53.Record(domain_name, subdomain, ui=ui, dry_run=dry_run)
	"""
	def __init__(self, domain_name, subdomain=None, ui=None, dry_run=False):
		self.dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
		self.client = boto3_client('route53', region_name="us-west-2")
		
		self.domain_name = domain_name
		if type(domain_name) is not str:
			self.zone = domain_name
			domain_name = self.zone.name
		else:
			self.zone = Zone(self.domain_name, ui=self.ui, dry_run=self.dry_run)
		
		if not self.zone:
			self.exists = False
			return
		self.name = subdomain
		self.record_name = self.zone.name
		if subdomain:
			self.record_name = self.name + '.' + self.zone.name
		
		self.info = {}
		if self.load():
			self.exists = True
		else:
			self.exists = False
	
	"""
	record_info = record.load()
	"""
	def load(self):
		response = self.client.list_resource_record_sets(
			HostedZoneId = self.zone.id,
			StartRecordName = self.record_name,
# 			StartRecordType = 'CNAME',
			MaxItems = '50'
		)
		
		if common.is_success(response) and response.get('ResourceRecordSets'):
			for record in response['ResourceRecordSets']:
				record_name = record['Name'].rstrip('.')
				if record_name == self.record_name and record['Type'] in ['A', 'CNAME']:
					self.info = record
					return True
		return False
	
	@property
	def type(self):
		if not self.exists:
			return False
		return self.info.get('Type')
	
	"""
	response = record.create(args)
	"""
	def create(self, args):
		if not args or type(args) is not dict:
			raise AttributeError("Received invalid args.")
		if 'address' not in args or 'zone_id' not in args:
			raise AttributeError("Missing target address.")
		
		record_type = 'A'
		if 'type' in args:
			record_type = args['type']
		
		response = self.client.change_resource_record_sets(
			HostedZoneId = self.zone.id,
			ChangeBatch = {
				'Changes': [{
					'Action': 'CREATE',
					'ResourceRecordSet': {
						'Name': self.record_name,
						'Type': record_type,
						'AliasTarget': {
							'HostedZoneId': args['zone_id'],
							'DNSName': args['address'],
							'EvaluateTargetHealth': False
						}
					}
				}]
			}
		)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'ChangeInfo' in response and type(response['ChangeInfo']) is dict:
			self.info = self.load()
			self.exists = True
			return True
		return False
	
	"""
	response = record.delete()
	"""
	def delete(self):
		response = self.client.change_resource_record_sets(
			HostedZoneId = self.zone.id,
			ChangeBatch = {
				'Changes': [{
					'Action': 'DELETE',
					'ResourceRecordSet': {
						'Name': self.record_name,
						'Type': self.type,
						'AliasTarget': self.info['AliasTarget']
					}
				}]
			}
		)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'ChangeInfo' in response and type(response['ChangeInfo']) is dict:
			self.info = None
			self.exists = False
			return True
		return False
	
