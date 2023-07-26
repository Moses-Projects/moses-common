# print("Loaded QLDB module")

import datetime
import json
import re
import os

from pyion2json import ion_to_json
from pyqldb.config.retry_config import RetryConfig
from pyqldb.driver.qldb_driver import QldbDriver

import moses_common.__init__ as common
import moses_common.ui

"""
import moses_common.qldb
"""

class QLDB:
	"""
	qldb = moses_common.qldb.QLDB(ledger_name)
	qldb = moses_common.qldb.QLDB(ledger_name, log_level=5, dry_run=False)
	log_level
		6: All DO statements
		7: All SELECT and DO statements
	"""
	def __init__(self, ledger_name, log_level=5, dry_run=False):
		self._dry_run = dry_run
		self.log_level = log_level
		
		# Configure retry limit to 3
		self.retry_config = RetryConfig(retry_limit=3)
		
		try:
			# Initialize the driver
			self._qldb = QldbDriver(ledger_name=ledger_name, retry_config=self.retry_config)
		except:
			raise ConnectionError("Unable to connect to ledger {}".format(ledger_name))
		
		self._now = datetime.datetime.utcnow()
		self._ui = moses_common.ui.Interface()
		
	
	def _connect_to_qldb(self):
		if not self._qldb:
			return moses_common.qldb.QLDB(self._ledger_name)
	
	@property
	def log_level(self):
		return self._log_level
	
	@log_level.setter
	def log_level(self, value):
		self._log_level = common.normalize_log_level(value)
	
	@property
	def qldb(self):
		return self._qldb
	
	@property
	def tables(self):
		tables = []
		for table in self._qldb.list_tables():
			tables.append(table)
		return tables
	
	"""
	tables = qldb.list_tables()
	"""
	def list_tables(self):
		tables = []
		for table in self._qldb.list_tables():
			tables.append(table)
		return tables
	
	def recreate_table(self, table_name):
		self._qldb.execute_lambda(lambda executor: self.recreate_table_execute(executor, table_name))
	
	def recreate_table_execute(self, transaction_executor, table_name):
		self.drop_table_execute(transaction_executor, table_name)
		self.create_table_execute(transaction_executor, table_name)
	
	def create_table(self, table_name):
		self._qldb.execute_lambda(lambda executor: self.create_table_execute(executor, table_name))
	
	def create_table_execute(self, transaction_executor, table_name):
		sql = "CREATE TABLE {}".format(table_name)
		print(sql)
		cursor = transaction_executor.execute_statement(sql)
		for doc in cursor:
			return doc['tableId']
	
	def create_index(self, table_name, field_name):
		self._qldb.execute_lambda(lambda executor: self.create_index_execute(executor, table_name, field_name))
	
	def create_index_execute(self, transaction_executor, table_name, field_name):
		sql = "CREATE INDEX ON {}({})".format(table_name, field_name)
		print(sql)
		cursor = transaction_executor.execute_statement(sql)
		for doc in cursor:
			return doc['tableId']
	
	def drop_table(self, table_name):
		self._qldb.execute_lambda(lambda executor: self.drop_table_execute(executor, table_name))
	
	def drop_table_execute(self, transaction_executor, table_name):
		sql = "DROP TABLE {}".format(table_name)
		print(sql)
		cursor = transaction_executor.execute_statement(sql)
		for doc in cursor:
			return doc['tableId']
	
	def deionize(self, doc):
		if type(doc) is str or type(doc) is int or type(doc) is float:
			return doc
		new_doc = ion_to_json(doc)
		if re.search(r'.IonPyInt', str(type(new_doc))):
			new_doc = int(new_doc)
		elif type(new_doc) is dict:
			for key, value in new_doc.items():
				if re.search(r'.IonPyInt', str(type(value))):
					new_doc[key] = int(value)
		return new_doc
	
	"""
	doc_ids = qldb.get_all_doc_ids(table_name)
	doc_ids = qldb.execute_lambda(lambda executor: qldb.exe_get_all_doc_ids(executor, table_name))
	
	ion_records = qldb.execute_lambda(lambda executor: qldb.qldb_get_all_doc_ids(executor, table_name))
	"""
	def get_all_doc_ids(self, table_name):
		return self._qldb.execute_lambda(lambda executor: self.exe_get_all_doc_ids(executor, table_name))

	def exe_get_all_doc_ids(self, executor, table_name):
		results = self.qldb_get_all_doc_ids(executor, table_name)
		records = []
		for doc_id in results:
			records.append(self.deionize(doc_id))
		return records
	
	def qldb_get_all_doc_ids(self, transaction_executor, table_name):
		sql = "SELECT doc_id FROM {} BY doc_id".format(table_name)
		cursor = transaction_executor.execute_statement(sql)
		records = []
		for doc in cursor:
			records.append(doc['doc_id'])
		return records
	
	
	"""
	record = qldb.get_record(table_name, doc_id)
	record = qldb.execute_lambda(lambda executor: qldb.exe_get_record(executor, table_name, doc_id))
	
	list = qldb.get_records_as_list(table_name, doc_id_list)
	list = qldb.execute_lambda(lambda executor: qldb.exe_get_records_as_list(executor, table_name, doc_id_list))
	
	hash = qldb.get_records_as_hash(table_name, doc_id_list)
	hash = qldb.execute_lambda(lambda executor: qldb.exe_get_records_as_hash(executor, table_name, doc_id_list))
	
	ion_records = qldb.execute_lambda(lambda executor: qldb.qldb_get_records(executor, table_name, doc_id_list))
	"""
	def get_record(self, table_name, doc_id, should_get_full_record=False):
		return self._qldb.execute_lambda(lambda executor: self.exe_get_record(executor, table_name, doc_id, should_get_full_record))
	
	def exe_get_record(self, executor, table_name, doc_id, should_get_full_record=False):
		results = self.qldb_get_records(executor, table_name, doc_id, should_get_full_record)
		for doc in results:
			return self.deionize(doc)
		
	def get_records_as_list(self, table_name, doc_id_list, should_get_full_record=False):
		return self._qldb.execute_lambda(lambda executor: self.exe_get_records_as_list(executor, table_name, doc_id_list, should_get_full_record))
	
	def exe_get_records_as_list(self, executor, table_name, doc_id_list, should_get_full_record=False):
		results = self.qldb_get_records(executor, table_name, doc_id_list, should_get_full_record)
		records = []
		for doc in results:
			records.append(self.deionize(doc))
		return records
		
	def get_records_as_hash(self, table_name, doc_id_list, should_get_full_record=False):
		return self._qldb.execute_lambda(lambda executor: self.exe_get_records_as_hash(executor, table_name, doc_id_list, should_get_full_record))
	
	def exe_get_records_as_hash(self, executor, table_name, doc_id_list, should_get_full_record=False):
		results = self.qldb_get_records(executor, table_name, doc_id_list, should_get_full_record)
		records = {}
		for doc in results:
			records[doc['doc_id']] = self.deionize(doc)
		return records
	
	def qldb_get_records(self, transaction_executor, table_name, doc_id_list, should_get_full_record=False):
		if type(doc_id_list) is not list:
			doc_id_list = [doc_id_list]
		list_string = (len(doc_id_list) - 1) * '?, ' + '?'
		if should_get_full_record:
			sql = "SELECT * FROM _ql_committed_{} BY doc_id WHERE doc_id IN ({})".format(table_name, list_string)
		else:
			sql = "SELECT * FROM {} BY doc_id WHERE doc_id IN ({})".format(table_name, list_string)
		if self._log_level >= 7:
			print(sql)
			print(doc_id_list)
		cursor = transaction_executor.execute_statement(sql, *doc_id_list)
		records = []
		for doc in cursor:
			records.append(doc)
		return records
	
	
	"""
	record = qldb.get_record_by_id(table_name, id)
	record = qldb.execute_lambda(lambda executor: qldb.exe_get_record_by_id(executor, table_name, id))
	
	list = qldb.get_records_by_id_as_list(table_name, doc_id_list)
	list = qldb.execute_lambda(lambda executor: qldb.exe_get_records_by_id_as_list(executor, table_name, doc_id_list))
	
	hash = qldb.get_records_by_id_as_hash(table_name, doc_id_list)
	hash = qldb.execute_lambda(lambda executor: qldb.exe_get_records_by_id_as_hash(executor, table_name, doc_id_list))
	
	ion_records = qldb.execute_lambda(lambda executor: qldb.qldb_get_records_by_id(executor, table_name, id))
	"""
	def get_record_by_id(self, table_name, id, should_get_full_record=False):
		return self._qldb.execute_lambda(lambda executor: self.exe_get_record_by_id(executor, table_name, id, should_get_full_record))
	
	def exe_get_record_by_id(self, executor, table_name, id, should_get_full_record=False):
		results = self.qldb_get_records_by_id(executor, table_name, id, should_get_full_record)
		for doc in results:
			return self.deionize(doc)
	
	def get_records_by_id_as_list(self, table_name, id_list, should_get_full_record=False):
		return self._qldb.execute_lambda(lambda executor: self.exe_get_records_by_id_as_list(executor, table_name, id_list, should_get_full_record))
	
	def exe_get_records_by_id_as_list(self, executor, table_name, id_list, should_get_full_record=False):
		results = self.qldb_get_records_by_id(executor, table_name, id_list, should_get_full_record)
		records = []
		for doc in results:
			records.append(self.deionize(doc))
		return records
		
	def get_records_by_id_as_hash(self, table_name, id_list, should_get_full_record=False):
		return self._qldb.execute_lambda(lambda executor: self.exe_get_records_by_id_as_hash(executor, table_name, id_list, should_get_full_record))
	
	def exe_get_records_by_id_as_hash(self, executor, table_name, doc_id_list, should_get_full_record=False):
		results = self.qldb_get_records_by_id(executor, table_name, doc_id_list, should_get_full_record)
		records = {}
		for doc in results:
			if should_get_full_record:
				records[doc['data']['id']] = self.deionize(doc)
			else:
				records[doc['id']] = self.deionize(doc)
		return records
	
	def qldb_get_records_by_id(self, transaction_executor, table_name, id_list, should_get_full_record=False):
		if type(id_list) is not list:
			id_list = [id_list]
		list_string = (len(id_list) - 1) * '?, ' + '?'
		if should_get_full_record:
			sql = "SELECT * FROM _ql_committed_{} BY doc_id WHERE data.id IN ({})".format(table_name, list_string)
		else:
			sql = "SELECT * FROM {} BY doc_id WHERE id IN ({})".format(table_name, list_string)
		if self._log_level >= 7:
			print(sql)
			print(id_list)
		cursor = transaction_executor.execute_statement(sql, *id_list)
		records = []
		for doc in cursor:
			records.append(doc)
		return records
	
	
	"""
	list = qldb.get_sql_as_column(sql, arg_list=None, key_name='doc_id')
	list = qldb.execute_lambda(lambda executor: qldb.exe_get_sql_as_column(executor, sql, arg_list=None, key_name='doc_id')
	
	record = qldb.get_sql_record(sql, arg_list=None)
	record = qldb.execute_lambda(lambda executor: qldb.exe_get_sql_record(executor, sql, arg_list=None))
	
	list = qldb.get_sql_as_list(sql, arg_list)
	list = qldb.execute_lambda(lambda executor: qldb.exe_get_sql_as_list(executor, sql, arg_list))
	
	hash = qldb.get_sql_as_hash(sql, arg_list, key_name='doc_id')
	hash = qldb.execute_lambda(lambda executor: qldb.exe_get_sql_as_hash(executor, sql, arg_list, key_name='doc_id'))
	
	ion_records = qldb.execute_lambda(lambda executor: qldb.qldb_get_sql(executor, sql, arg_list))
	"""
	def get_sql_as_column(self, sql, arg_list, key_name='doc_id'):
		return self._qldb.execute_lambda(lambda executor: self.exe_get_sql_as_column(executor, sql, arg_list, key_name))
	
	def exe_get_sql_as_column(self, executor, sql, arg_list, key_name='doc_id'):
		results = self.qldb_get_sql(executor, sql, arg_list)
		records = []
		for doc in results:
			records.append(self.deionize(doc[key_name]))
		return records
	
	def get_sql_record(self, sql, arg_list):
		return self._qldb.execute_lambda(lambda executor: self.exe_get_sql_record(executor, sql, arg_list))
	
	def exe_get_sql_record(self, executor, sql, arg_list):
		results = self.qldb_get_sql(executor, sql, arg_list)
		for doc in results:
			return self.deionize(doc)
		
	def get_sql_as_list(self, sql, arg_list):
		return self._qldb.execute_lambda(lambda executor: self.exe_get_sql_as_list(executor, sql, arg_list))
	
	def exe_get_sql_as_list(self, executor, sql, arg_list):
		results = self.qldb_get_sql(executor, sql, arg_list)
		records = []
		for doc in results:
			records.append(self.deionize(doc))
		return records
		
	def get_sql_as_hash(self, sql, arg_list, key_name='doc_id'):
		return self._qldb.execute_lambda(lambda executor: self.exe_get_sql_as_hash(executor, sql, arg_list, key_name))
	
	def exe_get_sql_as_hash(self, executor, sql, arg_list, key_name='doc_id'):
		results = self.qldb_get_sql(executor, sql, arg_list)
		records = {}
		for doc in results:
			records[doc[key_name]] = self.deionize(doc)
		return records
	
	def qldb_get_sql(self, transaction_executor, sql, arg_list):
		if type(arg_list) is not list:
			arg_list = [arg_list]
		if self._log_level >= 7:
			print(sql)
			print(arg_list)
		cursor = transaction_executor.execute_statement(sql, *arg_list)
		records = []
		for doc in cursor:
			records.append(doc)
		return records
	
	
	"""
	doc_id = qldb.insert_record(table_name, item)
	doc_id = qldb.execute_lambda(lambda executor: qldb.exe_insert_records(executor, table_name, item))
	ion_doc_id = qldb.execute_lambda(lambda executor: qldb.qldb_insert_records(executor, table_name, item))
	"""
	def insert_record(self, table_name, item):
		return self._qldb.execute_lambda(lambda executor: self.exe_insert_records(executor, table_name, item))
	
	def insert_records(self, table_name, item_list):
		return self._qldb.execute_lambda(lambda executor: self.exe_insert_records(executor, table_name, item_list))
	
	def exe_insert_records(self, executor, table_name, item):
		doc_id = self.qldb_insert_records(executor, table_name, item)
		if doc_id:
			return self.deionize(doc_id)
		return None
	
	def qldb_insert_records(self, transaction_executor, table_name, item):
		sql = "INSERT INTO {} ?".format(table_name)
		if self._dry_run:
			self._ui.dry_run(sql)
			self._ui.dry_run(item)
			return 'xxx'
		if self._log_level >= 7:
			print(sql)
			print(item)
		cursor = transaction_executor.execute_statement(sql, item)
		for doc in cursor:
			if 'documentId' in doc:
				if self._log_level >= 6:
					print("QLDB inserted {} into {}".format(doc['documentId'], table_name))
				return doc['documentId']
		if self._log_level >= 6:
			print("QLDB insert failed on", table_name)
		return 
	
	
	"""
	doc_id = qldb.update_record(table_name, doc_id, item)
	doc_ids = qldb.update_records(table_name, doc_id_list, item)
	doc_id = qldb.execute_lambda(lambda executor: qldb.exe_update_records(executor, table_name, doc_id, item))
	doc_ids = qldb.execute_lambda(lambda executor: qldb.exe_update_records(executor, table_name, doc_id_list, item))
	ion_doc_id = qldb.execute_lambda(lambda executor: qldb.qldb_update_records(executor, table_name, doc_id, item))
	ion_doc_ids = qldb.execute_lambda(lambda executor: qldb.qldb_update_records(executor, table_name, doc_id_list, item))
	"""
	def update_record(self, table_name, doc_id_list, item):
		return self._qldb.execute_lambda(lambda executor: self.exe_update_records(executor, table_name, doc_id_list, item))
	
	def update_records(self, table_name, doc_id_list, item):
		return self._qldb.execute_lambda(lambda executor: self.exe_update_records(executor, table_name, doc_id_list, item))
	
	def exe_update_records(self, executor, table_name, doc_id_list, item):
		doc_ids = self.qldb_update_records(executor, table_name, doc_id_list, item)
		if doc_ids:
			if type(doc_ids) is list:
				return_doc_ids = []
				for doc_id in doc_ids:
					return_doc_ids.append(self.deionize(doc_id))
				return return_doc_ids
			else:
				return self.deionize(doc_ids)
		return None
	
	def qldb_update_records(self, transaction_executor, table_name, doc_id_list, item):
# 		print("item {}: {}".format(type(item), item))
		is_multiple = True
		if type(doc_id_list) is not list:
			doc_id_list = [doc_id_list]
			is_multiple = False
		list_string = (len(doc_id_list) - 1) * '?, ' + '?'
		
		set_list = []
		args = []
		for key, value in item.items():
			if key == 'doc_id':
				continue
			qvalue = 'NULL'
			if type(value) is bool:
				if value:
					qvalue = 'TRUE'
				else:
					qvalue = 'FALSE'
			elif type(value) is int:
				qvalue = str(value)
			elif type(value) is str:
				qvalue = "'{}'".format(value)
			elif type(value) is type(self._now):
				qvalue = "'{}'".format(value.isoformat())
			elif type(value) is list or type(value) is dict:
				args.append(value)
				qvalue = "?"
			set_list.append('{} = {}'.format(key, qvalue))
		if not len(set_list):
			return True
		
		doc_id_list = args + doc_id_list
		
		sql = "UPDATE {} BY doc_id SET {} WHERE doc_id IN ({})".format(table_name, ', '.join(set_list), list_string)
		if self._dry_run:
			self._ui.dry_run(sql)
			self._ui.dry_run(doc_id_list)
			if is_multiple:
				return ['xxx']
			return 'xxx'
		if self._log_level >= 7:
			print(sql)
			print(doc_id_list)
		cursor = transaction_executor.execute_statement(sql, *doc_id_list)
		if is_multiple:
			return_doc_ids = []
			for doc in cursor:
				if 'documentId' in doc:
					if self._log_level >= 6:
						print("QLDB updated {} in {}".format(doc['documentId'], table_name))
					return_doc_ids.append(doc['documentId'])
			return return_doc_ids
		else:
			for doc in cursor:
				if 'documentId' in doc:
					if self._log_level >= 6:
						print("QLDB updated {} in {}".format(doc['documentId'], table_name))
					return doc['documentId']
		if self._log_level >= 6:
			print("QLDB update failed on", table_name)
		return 
	
	
