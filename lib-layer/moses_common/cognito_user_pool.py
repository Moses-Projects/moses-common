# print("Loaded CodeBuild module")

import json
from botocore.exceptions import ClientError
from boto3 import client as boto3_client

import moses_common.__init__ as common
import moses_common.ui

class UserPool:
	"""
	import moses_common.cognito_user_pool
	user_pool = moses_common.cognito_user_pool.UserPool(user_pool_name, ui=ui, dry_run=dry_run)
	"""
	def __init__(self, user_pool_name, ui=None, dry_run=False):
		self.dry_run = dry_run
		
		self.ui = ui or moses_common.ui.Interface()
		self.client = boto3_client('cognito-idp', region_name="us-west-2")
		
		self.name = user_pool_name
		self.info = None
		self.client_list = None
		self.clients = {}
#		self.domain = None
		self.managed_login = None
		if self.load():
			self.exists = True
			self.load_clients()
			self.managed_login = self.get_managed_login()
			self.risk_configuration = self.get_risk_configuration()
			self.log_configuration = self.get_log_configuration()
		else:
			self.exists = False
	
	"""
	user_pool_info = user_pool.load()
	"""
	def load(self):
		# List of user pools
		paginator = self.client.get_paginator('list_user_pools')
		user_pool_id = None
		for page in paginator.paginate(MaxResults=60):
			for pool in page['UserPools']:
				if pool['Name'] == self.name:
					user_pool_id = pool['Id']
					break
			if user_pool_id:
				break
	
		if not user_pool_id:
			return False
	
		# Describe user pool
		self.info = self.client.describe_user_pool(UserPoolId=user_pool_id)['UserPool']
	
		# List clients
		self.client_list = []
		client_paginator = self.client.get_paginator('list_user_pool_clients')
		for page in client_paginator.paginate(UserPoolId=user_pool_id, MaxResults=60):
			self.client_list.extend(page['UserPoolClients'])
		
		# Get managed login branding
#		try:
#			self.managed_login = self.client.describe_managed_login_branding(UserPoolId=user_pool_id, ManagedLoginBrandingId='fb28f1c3-0f83-4e87-8147-1ef0c9c18f9a')['ManagedLoginBranding']
#		except self.client.exceptions.ResourceNotFoundException:
#			self.managed_login = None

		return True
	
	def load_clients(self):
		if type(self.client_list) is not list:
			return
		for client in self.client_list:
			response = self.client.describe_user_pool_client(
				UserPoolId = self.id,
				ClientId = client['ClientId']
			)
			if common.is_success(response) and response.get('UserPoolClient'):
				self.clients[response['UserPoolClient']['ClientName']] = response['UserPoolClient']
		return 
	
	def get_domain_info(self):
		try:
			response = self.client.describe_user_pool_domain(
				Domain = self.custom_domain
			)
			if common.is_success(response) and response.get('DomainDescription'):
				return response['DomainDescription']
		except self.client.exceptions.ResourceNotFoundException:
			return None
	
	def get_managed_login(self):
		try:
			response = self.client.describe_managed_login_branding_by_client(
				UserPoolId = self.id,
				ClientId = self.client_list[0]['ClientId'],
				ReturnMergedResources = False
			)
			if common.is_success(response) and response.get('ManagedLoginBranding'):
				return response['ManagedLoginBranding']
		except self.client.exceptions.ResourceNotFoundException:
			return None
	
	def get_risk_configuration(self, client_id=None):
		try:
			if client_id:
				response = self.client.describe_risk_configuration(
					UserPoolId = self.id,
					ClientId = client_id
				)
			else:
				response = self.client.describe_risk_configuration(
					UserPoolId = self.id
				)
			if common.is_success(response) and response.get('RiskConfiguration'):
				return response['RiskConfiguration']
		except self.client.exceptions.ResourceNotFoundException:
			return None
	
	def get_log_configuration(self):
		try:
			response = self.client.get_log_delivery_configuration(
				UserPoolId = self.id
			)
			if common.is_success(response) and response.get('LogDeliveryConfiguration') and response['LogDeliveryConfiguration'].get('LogConfigurations'):
				return response['LogDeliveryConfiguration']['LogConfigurations']
		except self.client.exceptions.ResourceNotFoundException:
			return None
	
	@property
	def arn(self):
		if not self.exists or 'Arn' not in self.info:
			return None
		return self.info['Arn']
	
	@property
	def id(self):
		if not self.exists or 'Id' not in self.info:
			return None
		return self.info['Id']
	
	@property
	def status(self):
		if not self.exists or 'Status' not in self.info:
			return None
		return self.info['Status']
	
	@property
	def domain(self):
		if not self.exists or 'Domain' not in self.info:
			return None
		return self.info['Domain']
	
	@property
	def custom_domain(self):
		if not self.exists or 'CustomDomain' not in self.info:
			return None
		return self.info['CustomDomain']
	
	
	"""
	success = user_pool.create()
	"""
	def create(self, args=None):
		if self.dry_run:
			self.ui.dry_run(f"create() create_user_pool('{self.name}')")
			self.exists = True
			self.info = {}
			return True
		
		service_name = args.get('service_name') or 'service'
		response = self.client.create_user_pool(
			PoolName = self.name,
			Policies = {
				"PasswordPolicy": {
					"MinimumLength": 8,
					"RequireUppercase": True,
					"RequireLowercase": True,
					"RequireNumbers": True,
					"RequireSymbols": True,
					"PasswordHistorySize": 24,
					"TemporaryPasswordValidityDays": 7
				},
				"SignInPolicy": {
					"AllowedFirstAuthFactors": ["PASSWORD"]
				}
			},
			DeletionProtection = "ACTIVE",
			LambdaConfig = args.get('lambda_config'),
			AutoVerifiedAttributes = ["email"],
			UsernameAttributes = ["email"],
			SmsVerificationMessage = "Your {} verification code is {####}. By verifying you agree to receive messages. Text STOP to opt-out. Msg & data rates apply.".format(service_name),
			EmailVerificationMessage = "Your {} MFA verification code is {####}.".format(service_name),
			EmailVerificationSubject = "Your {} MFA verification code".format(service_name),
			VerificationMessageTemplate = {
				"SmsMessage": "Your {} verification code is {####}. By verifying you agree to receive messages. Text STOP to opt-out. Msg & data rates apply.".format(service_name),
				"EmailMessage": "Your {} MFA verification code is {####}.".format(service_name),
				"EmailSubject": "Your {} MFA verification code".format(service_name),
				"DefaultEmailOption": "CONFIRM_WITH_CODE"
			},
			SmsAuthenticationMessage = "Your {} authentication code is {####}.".format(service_name),
			MfaConfiguration = "ON",
			UserAttributeUpdateSettings={
				"AttributesRequireVerificationBeforeUpdate": ["email"]
			},
			DeviceConfiguration = {
				"ChallengeRequiredOnNewDevice": False,
				"DeviceOnlyRememberedOnUserPrompt": False
			},
			EmailConfiguration = args.get('email_configuration'),
			SmsConfiguration = args.get('sms_configuration'),
			UserPoolTags = {},
			AdminCreateUserConfig = {
				"AllowAdminCreateUserOnly": True,
				"InviteMessageTemplate": {
					"SMSMessage": "Your {} username is {username} and temporary password is {####}.".format(service_name),
					"EmailMessage": "Your {} username is {username} and temporary password is {####}.".format(service_name),
					"EmailSubject": "Your {} temporary password".format(service_name)
				}
			},
			Schema = [
				{
					"Name": "email",
					"AttributeDataType": "String",
					"DeveloperOnlyAttribute": False,
					"Mutable": True,
					"Required": False,
					"StringAttributeConstraints": {
						"MinLength": "0",
						"MaxLength": "2048"
					}
				},
				{
					"Name": "email_verified",
					"AttributeDataType": "Boolean",
					"DeveloperOnlyAttribute": False,
					"Mutable": True,
					"Required": False
				},
				{
					"Name": "updated_at",
					"AttributeDataType": "Number",
					"DeveloperOnlyAttribute": False,
					"Mutable": True,
					"Required": False,
					"NumberAttributeConstraints": {
						"MinValue": "0"
					}
				},
			],
			UserPoolAddOns = {
				"AdvancedSecurityMode": "ENFORCED",
				"AdvancedSecurityAdditionalFlows": {
					"CustomAuthMode": "ENFORCED"
	        	}
			},
			UsernameConfiguration = {
				"CaseSensitive": False
			},
			AccountRecoverySetting = {
				"RecoveryMechanisms": [
					{
						"Priority": 1,
						"Name": "verified_email"
					}
				]
			},
			UserPoolTier = "PLUS"
		)
#		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'UserPool' in response:
			self.info = response['UserPool']
			self.exists = True
			return True
		return False


	"""
	success = user_pool.update()
	"""
	def update(self, args=None):
		if self.dry_run:
			self.ui.dry_run(f"update() update_user_pool('{self.name}')")
			self.exists = True
			self.info = {}
			return True
		
		response = self.client.create_user_pool(
			PoolName = self.name,
			UserPoolAddOns = {
				"AdvancedSecurityMode": "ENFORCED"
			}
		)
		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'UserPool' in response:
			return True
		return False


	"""
	success = user_pool.create_client(client_name, {
		"callback_urls": callback_urls,
		"logout_urls": logout_urls
	})
	"""
	def create_client(self, client_name, args=None):
		if self.dry_run:
			self.ui.dry_run(f"create_client() create_user_pool_client('{client_name}')")
			self.clients[client_name] = {}
			return True
		
		response = self.client.create_user_pool_client(
			UserPoolId = self.id,
			ClientName = client_name,
			GenerateSecret = True,
			RefreshTokenValidity = 1,
			AccessTokenValidity = 60,
			IdTokenValidity = 60,
			TokenValidityUnits = {
				"AccessToken": "minutes",
				"IdToken": "minutes",
				"RefreshToken": "days"
			},
			ExplicitAuthFlows = [ "ALLOW_REFRESH_TOKEN_AUTH", "ALLOW_USER_AUTH", "ALLOW_USER_SRP_AUTH" ],
			SupportedIdentityProviders = [ "COGNITO" ],
			CallbackURLs = args.get('callback_urls'),
			LogoutURLs = args.get('logout_urls'),
			AllowedOAuthFlows = [ "code" ],
			AllowedOAuthScopes=[ "aws.cognito.signin.user.admin", "email", "openid", "phone" ],
			AllowedOAuthFlowsUserPoolClient = True,
			PreventUserExistenceErrors = "ENABLED",
			EnableTokenRevocation = True,
			EnablePropagateAdditionalUserContextData = False,
			AuthSessionValidity = 3,
		)
#		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'UserPoolClient' in response:
			self.clients[client_name] = response['UserPoolClient']
			return True
		return False


	"""
	success = user_pool.create_custom_domain(domain, cert_arn)
	"""
	def create_custom_domain(self, domain, cert_arn):
		if self.dry_run:
			self.ui.dry_run(f"create_custom_domain() create_user_pool_domain('{domain}')")
			self.info['CustomDomain'] = domain
			return True
		
		response = self.client.create_user_pool_domain(
			Domain = domain,
			UserPoolId = self.id,
			ManagedLoginVersion = 2,
			CustomDomainConfig = {
				"CertificateArn": cert_arn
			}
		)
		
#		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and 'CloudFrontDomain' in response:
			self.info['CustomDomain'] = domain
			return True
		return False


	"""
	success = user_pool.create_managed_login(settings, assets)
	"""
	def create_managed_login(self, settings, assets):
		if self.dry_run:
			self.ui.dry_run(f"create_managed_login() create_managed_login_branding()")
			return True
		
		response = self.client.create_managed_login_branding(
			UserPoolId = self.id,
			ClientId = self.client_list[0]['ClientId'],
			UseCognitoProvidedValues = True,
#			Settings = settings,
#			Assets = assets
		)
		
#		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and response.get('ManagedLoginBranding'):
			self.managed_login = response['ManagedLoginBranding']
			return True
		return False


	"""
	success = user_pool.update_managed_login(settings, assets)
	"""
	def update_managed_login(self, settings, assets):
		if self.dry_run:
			self.ui.dry_run(f"update_managed_login() update_managed_login_branding()")
			return True
		
		response = self.client.update_managed_login_branding(
			UserPoolId = self.id,
			ManagedLoginBrandingId = self.managed_login['ManagedLoginBrandingId'],
			Settings = settings,
			Assets = assets
		)
		
#		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and response.get('ManagedLoginBranding'):
			self.managed_login = response['ManagedLoginBranding']
			return True
		return False


	"""
	success = user_pool.set_risk_configuration(compromised_credentials, account_takeover, client_id=None)
	"""
	def set_risk_configuration(self, compromised_credentials, account_takeover, client_id=None):
		if self.dry_run:
			self.ui.dry_run(f"set_risk_configuration()")
			return True
		
		if client_id:
			response = self.client.set_risk_configuration(
				UserPoolId = self.id,
				ClientId = client_id,
				CompromisedCredentialsRiskConfiguration = compromised_credentials,
				AccountTakeoverRiskConfiguration = account_takeover
			)
		else:
			response = self.client.set_risk_configuration(
				UserPoolId = self.id,
				CompromisedCredentialsRiskConfiguration = compromised_credentials,
				AccountTakeoverRiskConfiguration = account_takeover
			)
		
# 		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and response.get('RiskConfiguration'):
			self.risk_configuration = response['RiskConfiguration']
			return True
		return False


	"""
	success = user_pool.set_log_configuration(log_group_arn)
	"""
	def set_log_configuration(self, log_group_arn):
		if self.dry_run:
			self.ui.dry_run(f"set_log_configuration() set_log_delivery_configuration({log_group_arn})")
			return True
		
		response = self.client.set_log_delivery_configuration(
			UserPoolId = self.id,
			LogConfigurations = [
				{
					"LogLevel": "INFO",
					"EventSource": "userAuthEvents",
					"CloudWatchLogsConfiguration": {
						"LogGroupArn": log_group_arn
					}
				}
			]
		)
		
		print("response {}: {}".format(type(response), response))
		if common.is_success(response) and response.get('LogDeliveryConfiguration') and response['LogDeliveryConfiguration'].get('LogConfigurations'):
			self.log_configuration = response['LogDeliveryConfiguration']['LogConfigurations']
			return True
		return False
