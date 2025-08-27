# ddb_action_processor.py

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

import moses_common.__init__ as common


def _utc_now_iso() -> str:
	"""
	Return an ISO 8601 UTC timestamp ending with 'Z'.
	"""
	return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class DynamoDBActionProcessor:
	"""
	Generic DynamoDB action processor that executes actions defined in an API schema.

	- Validates inputs using common.check_input()
	- Optional auth check per-action
	- GET uses GetItem when full table key is present; otherwise Query (table or index)
	- POST uses conditional PutItem (no overwrite)
	- PATCH updates only non-key fields; supports attribute deletion via None; always sets update_time
	- DELETE deletes by key with existence check
	- Timestamps are ISO 8601 UTC with 'Z'
	- key_schema in action definitions is optional; DescribeTable is used (and cached) if absent

	Action definition (relevant fields):
	{
		"source": "ddb",
		"table_name": "...",
		"index_name": "...",					# optional, for GET list queries
		"key_schema": {							# optional; if omitted, DescribeTable is used
			"table": { "pk": "pk_name", "sk": "sk_name" },  # "sk" optional if table has no sort key
			"indices": {
				"indexName": { "pk": "pk_name", "sk": "sk_name" }  # "sk" optional
			}
		},
		"fields": [ [name, type, required_or_list], ... ],
		"auth": {								# optional
			"table_name": "...",
			"index_name": "...",				# optional
			"key_schema": { ... },				# optional; DescribeTable if missing
			"fields": [ [name, type, required_or_list], ... ]  # used to gather values for auth
		}
	}
	"""

	def __init__(self, owner: Optional[str] = None, ddb_resource=None, ui=None, dry_run=None):
		self._dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
		
		self.owner = owner
		self.ddb = ddb_resource or boto3.resource("dynamodb")
		self.ddb_client = self.ddb.meta.client
		# Cache of discovered schemas by table name
		self._schema_cache: Dict[str, Dict[str, Any]] = {}

	# ---------- Public API ----------

	def process(self, method: str, action_def: Dict[str, Any], path_vars: Dict[str, Any], data_dict: Dict[str, Any]) -> Dict[str, Any]:
		"""
		Execute the action.

		:param method: HTTP method (GET, POST, PATCH, DELETE)
		:param action_def: The action definition from the API schema (see class docstring)
		:param path_vars: Dict of path variables (authoritative)
		:param data_dict: Dict of query/body parameters (used for fields not in path)
		:return: normalized success dict or {"errors":[...]} on failure
		"""
		try:
			if action_def.get("source") != "ddb":
				return self._error("bad_request", "Unsupported source in action", details={"source": action_def.get("source")})

			table_name = action_def.get("table_name")
			if not table_name:
				return self._error("bad_request", "Missing table_name in action")

			# Merge inputs: path vars override data_dict
			merged_input = self._merge_inputs(path_vars, data_dict)

			# Validate inputs for the action
			field_defs = action_def.get("fields", [])
			validated, input_errors = common.check_input(field_defs, merged_input)
			if input_errors:
				return self._error("bad_request", "Validation failed", details=input_errors)

			# Perform optional auth check
			auth_block = action_def.get("auth")
			if auth_block:
				auth_ok, auth_err = self._perform_auth_check(auth_block, path_vars, data_dict)
				if not auth_ok:
					# Return structured auth error
					return self._error("auth_failed", auth_err or "Authorization failed")

			# Resolve table and schema (table + indices)
			table_obj = self.ddb.Table(table_name)
			schema = self._get_or_describe_schema(table_name, action_def.get("key_schema"))

			method_upper = method.upper().strip()
			if method_upper == "GET":
				return self._handle_get(action_def, table_obj, schema, validated, data_dict)
			elif method_upper == "POST":
				return self._handle_post(table_obj, schema, validated)
			elif method_upper == "PATCH":
				return self._handle_patch(table_obj, schema, validated)
			elif method_upper == "DELETE":
				return self._handle_delete(table_obj, schema, validated)
			else:
				return self._error("bad_request", f"Unsupported method: {method}")
		except ClientError as e:
			return self._handle_client_error(e)
		except Exception as e:
			# Unknown error; do not leak stack traces
			return self._error("internal_error", "Unexpected error", details=str(e))

	# ---------- Internal helpers ----------

	def _merge_inputs(self, path_vars: Dict[str, Any], data_dict: Dict[str, Any]) -> Dict[str, Any]:
		merged = dict(data_dict or {})
		# Path vars are authoritative
		for k, v in (path_vars or {}).items():
			merged[k] = v
		# If owner was supplied at init and not provided explicitly, supply it (non-authoritative)
		if self.owner is not None and "owner" not in merged:
			merged["owner"] = self.owner
		return merged

	def _get_or_describe_schema(self, table_name: str, declared_key_schema: Optional[Dict[str, Any]]) -> Dict[str, Any]:
		"""
		Return a schema mapping:
		{
			"table": {"pk": "pk_name", "sk": "sk_name_or_None"},
			"indices": {"IndexName": {"pk": "...", "sk": "... or None"}, ...}
		}
		If declared_key_schema is present, use it; otherwise DescribeTable (cached).
		"""
		if declared_key_schema:
			# Normalize to our internal shape
			table_keys = declared_key_schema.get("table", {})
			indices = declared_key_schema.get("indices", {})
			return {
				"table": {"pk": table_keys.get("pk"), "sk": table_keys.get("sk")},
				"indices": {name: {"pk": d.get("pk"), "sk": d.get("sk")} for name, d in (indices or {}).items()}
			}

		if table_name in self._schema_cache:
			return self._schema_cache[table_name]

		desc = self.ddb_client.describe_table(TableName=table_name)["Table"]
		table_pk, table_sk = self._extract_keypair(desc.get("KeySchema", []))
		indices: Dict[str, Dict[str, Optional[str]]] = {}

		for gsi in desc.get("GlobalSecondaryIndexes", []) or []:
			name = gsi.get("IndexName")
			pk, sk = self._extract_keypair(gsi.get("KeySchema", []))
			indices[name] = {"pk": pk, "sk": sk}

		for lsi in desc.get("LocalSecondaryIndexes", []) or []:
			name = lsi.get("IndexName")
			pk, sk = self._extract_keypair(lsi.get("KeySchema", []))
			indices[name] = {"pk": pk, "sk": sk}

		schema = {
			"table": {"pk": table_pk, "sk": table_sk},
			"indices": indices
		}
		self._schema_cache[table_name] = schema
		return schema

	@staticmethod
	def _extract_keypair(key_schema_list: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
		pk = sk = None
		for ks in key_schema_list or []:
			if ks.get("KeyType") == "HASH":
				pk = ks.get("AttributeName")
			elif ks.get("KeyType") == "RANGE":
				sk = ks.get("AttributeName")
		return pk, sk

	def _perform_auth_check(self, auth_block: Dict[str, Any], path_vars: Dict[str, Any], data_dict: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
		"""
		Validate auth input, then Query the auth table (optionally via index).
		If at least one row matches (after optional attribute filters), auth passes.
		"""
		merged = self._merge_inputs(path_vars, data_dict)
		field_defs = auth_block.get("fields", [])
		validated, errors = common.check_input(field_defs, merged)
		if errors:
			return False, "Auth validation failed"

		auth_table_name = auth_block.get("table_name")
		if not auth_table_name:
			return False, "Auth misconfigured: missing table_name"

		# Resolve schema for the auth target
		auth_schema = self._get_or_describe_schema(auth_table_name, auth_block.get("key_schema"))
		auth_table = self.ddb.Table(auth_table_name)
		index_name = auth_block.get("index_name")

		# Determine keys for query (index if provided, else table)
		if index_name:
			index_keys = auth_schema["indices"].get(index_name)
			if not index_keys:
				return False, f"Auth misconfigured: unknown index '{index_name}'"
			pk_name = index_keys.get("pk")
			sk_name = index_keys.get("sk")
		else:
			pk_name = auth_schema["table"].get("pk")
			sk_name = auth_schema["table"].get("sk")

		if not pk_name:
			return False, "Auth misconfigured: missing partition key"

		# Build KeyConditionExpression from provided values
		if pk_name not in validated:
			return False, f"Auth requires '{pk_name}'"

		key_cond = Key(pk_name).eq(validated[pk_name])
		if sk_name and sk_name in validated:
			key_cond = key_cond & Key(sk_name).eq(validated[sk_name])

		# Build optional FilterExpression for any other provided auth fields
		filter_expr = None
		for name in (v[0] for v in field_defs):
			if name not in (pk_name, sk_name) and name in validated:
				expr = Attr(name).eq(validated[name])
				filter_expr = expr if filter_expr is None else (filter_expr & expr)

		kwargs = {
			"KeyConditionExpression": key_cond
		}
		if index_name:
			kwargs["IndexName"] = index_name
		if filter_expr is not None:
			kwargs["FilterExpression"] = filter_expr
		kwargs["Limit"] = 1

		resp = auth_table.query(**kwargs)
		count = resp.get("Count", 0)
		return (count > 0, None if count > 0 else "Authorization failed")

	# ---------- Method handlers ----------

	def _handle_get(self, action_def: Dict[str, Any], table, schema: Dict[str, Any], validated: Dict[str, Any], raw_input: Dict[str, Any]) -> Dict[str, Any]:
		"""
		GET semantics:
		- If full table key present: GetItem
		- Else if index_name present: Query on that index (PK required; SK optional)
		- Else:
			- If only table PK present and table has SK: Query base table by PK
			- If only table PK present and table no SK: GetItem by PK
			- If insufficient key info: error
		Supports 'limit' and 'last_evaluated_key' (raw_input).
		"""
		index_name = action_def.get("index_name")
		table_pk = schema["table"].get("pk")
		table_sk = schema["table"].get("sk")

		# If we have a full table key, try GetItem
		if table_pk and table_pk in validated and ((table_sk is None) or (table_sk in validated)):
			key = {table_pk: validated[table_pk]}
			if table_sk:
				key[table_sk] = validated[table_sk]
			resp = table.get_item(Key=key)
			item = resp.get("Item")
			if not item:
				return self._error("not_found", "Item not found")
			return {"item": item}

		# Else perform a Query (index or table)
		if index_name:
			indices = schema.get("indices", {})
			idx = indices.get(index_name)
			if not idx:
				return self._error("bad_request", f"Unknown index '{index_name}'")
			pk_name = idx.get("pk")
			sk_name = idx.get("sk")
			if not pk_name or pk_name not in validated:
				return self._error("bad_request", f"Missing required index partition key '{pk_name}'")
			key_cond = Key(pk_name).eq(validated[pk_name])
			if sk_name and sk_name in validated:
				key_cond = key_cond & Key(sk_name).eq(validated[sk_name])
			return self._paged_query(table, key_cond, raw_input, index_name=index_name)

		# Query base table by PK if possible
		if table_pk and table_pk in validated:
			if table_sk:
				key_cond = Key(table_pk).eq(validated[table_pk])
				return self._paged_query(table, key_cond, raw_input, index_name=None)
			else:
				# No SK in table: a GetItem by PK is correct for a single row table
				resp = table.get_item(Key={table_pk: validated[table_pk]})
				item = resp.get("Item")
				if not item:
					return self._error("not_found", "Item not found")
				return {"item": item}

		return self._error("bad_request", "Insufficient key information for GET")

	def _handle_post(self, table, schema: Dict[str, Any], validated: Dict[str, Any]) -> Dict[str, Any]:
		table_pk = schema["table"].get("pk")
		table_sk = schema["table"].get("sk")
		if not table_pk:
			return self._error("bad_request", "Table partition key is undefined")

		# Ensure keys are present
		if table_pk not in validated or (table_sk and table_sk not in validated):
			return self._error("bad_request", "Missing required key(s) for create")

		# Build item: include all validated fields
		item = dict(validated)
		now = _utc_now_iso()
		item.setdefault("create_time", now)
		item.setdefault("update_time", now)

		# Conditional: must not already exist
		cond = f"attribute_not_exists({table_pk})"
		if table_sk:
			cond = cond + f" AND attribute_not_exists({table_sk})"

		try:
			table.put_item(Item=item, ConditionExpression=cond)
			return {"item": item}
		except ClientError as e:
			if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
				return self._error("already_exists", "Item already exists")
			raise

	def _handle_patch(self, table, schema: Dict[str, Any], validated: Dict[str, Any]) -> Dict[str, Any]:
		table_pk = schema["table"].get("pk")
		table_sk = schema["table"].get("sk")
		if not table_pk:
			return self._error("bad_request", "Table partition key is undefined")

		if table_pk not in validated or (table_sk and table_sk not in validated):
			return self._error("bad_request", "Missing required key(s) for update")

		key = {table_pk: validated[table_pk]}
		if table_sk:
			key[table_sk] = validated[table_sk]

		# Determine non-key fields present in validated
		non_keys = {k: v for k, v in validated.items() if k not in key}

		# Always set update_time
		non_keys["update_time"] = _utc_now_iso()

		# Build UpdateExpression with SET and DELETE
		set_expr_parts = []
		del_expr_names = []
		expr_attr_names = {}
		expr_attr_values = {}

		name_counter = 0
		value_counter = 0

		for field, value in non_keys.items():
			# Skip keys defensively
			if field in key:
				continue
			if value is None:
				# DELETE attribute
				del_expr_names.append(field)
				continue
			# SET attribute
			name_token = f"#n{name_counter}"
			val_token = f":v{value_counter}"
			name_counter += 1
			value_counter += 1
			expr_attr_names[name_token] = field
			expr_attr_values[val_token] = value
			set_expr_parts.append(f"{name_token} = {val_token}")

		if not set_expr_parts and not del_expr_names:
			return self._error("no_fields", "No updatable fields provided")

		update_expr = []
		if set_expr_parts:
			update_expr.append("SET " + ", ".join(set_expr_parts))
		if del_expr_names:
			# Map each delete name to an expression name
			del_name_tokens = []
			for field in del_expr_names:
				name_token = f"#n{name_counter}"
				name_counter += 1
				expr_attr_names[name_token] = field
				del_name_tokens.append(name_token)
			update_expr.append("DELETE " + ", ".join(del_name_tokens))

		update_expr_str = " ".join(update_expr)

		# Ensure the item exists
		cond = f"attribute_exists({table_pk})"
		if table_sk:
			cond = cond + f" AND attribute_exists({table_sk})"

		try:
			resp = table.update_item(
				Key=key,
				UpdateExpression=update_expr_str,
				ExpressionAttributeNames=expr_attr_names or None,
				ExpressionAttributeValues=expr_attr_values or None,
				ConditionExpression=cond,
				ReturnValues="ALL_NEW"
			)
			return {"item": resp.get("Attributes")}
		except ClientError as e:
			if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
				return self._error("not_found", "Item not found")
			raise

	def _handle_delete(self, table, schema: Dict[str, Any], validated: Dict[str, Any]) -> Dict[str, Any]:
		table_pk = schema["table"].get("pk")
		table_sk = schema["table"].get("sk")
		if not table_pk:
			return self._error("bad_request", "Table partition key is undefined")

		if table_pk not in validated or (table_sk and table_sk not in validated):
			return self._error("bad_request", "Missing required key(s) for delete")

		key = {table_pk: validated[table_pk]}
		if table_sk:
			key[table_sk] = validated[table_sk]

		# Ensure exists; return deleted key on success
		cond = f"attribute_exists({table_pk})"
		if table_sk:
			cond = cond + f" AND attribute_exists({table_sk})"

		try:
			resp = table.delete_item(
				Key=key,
				ConditionExpression=cond,
				ReturnValues="ALL_OLD"
			)
			if "Attributes" not in resp:
				# Shouldn't happen with condition, but handle gracefully
				return self._error("not_found", "Item not found")
			return {"deleted": key}
		except ClientError as e:
			if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
				return self._error("not_found", "Item not found")
			raise

	# ---------- Query/pagination helpers ----------

	def _paged_query(self, table, key_condition, raw_input: Dict[str, Any], index_name: Optional[str]) -> Dict[str, Any]:
		kwargs: Dict[str, Any] = {"KeyConditionExpression": key_condition}
		if index_name:
			kwargs["IndexName"] = index_name

		# Pagination inputs: limit, last_evaluated_key
		limit = raw_input.get("limit")
		if isinstance(limit, int) and limit > 0:
			kwargs["Limit"] = limit

		lek = raw_input.get("last_evaluated_key")
		if isinstance(lek, str):
			# Support JSON-encoded string for convenience
			try:
				lek = json.loads(lek)
			except Exception:
				pass
		if isinstance(lek, dict):
			kwargs["ExclusiveStartKey"] = lek

		resp = table.query(**kwargs)
		items = resp.get("Items", [])
		out: Dict[str, Any] = {
			"items": items,
			"count": resp.get("Count", len(items))
		}
		if "LastEvaluatedKey" in resp:
			out["last_evaluated_key"] = resp["LastEvaluatedKey"]
		else:
			out["last_evaluated_key"] = None
		return out

	# ---------- Error helpers ----------

	def _handle_client_error(self, e: ClientError) -> Dict[str, Any]:
		err = e.response.get("Error", {})
		code = err.get("Code")
		msg = err.get("Message", "DynamoDB error")

		# Map common cases we didn't already catch
		if code == "ProvisionedThroughputExceededException":
			return self._error("throttled", "Throughput exceeded", details=msg)
		if code == "ValidationException":
			return self._error("bad_request", "Validation error", details=msg)
		if code == "AccessDeniedException":
			return self._error("forbidden", "Access denied", details=msg)

		# Fallback
		return self._error("ddb_error", "DynamoDB error", details={"code": code, "message": msg})

	@staticmethod
	def _error(code: str, message: str, details: Any = None) -> Dict[str, Any]:
		err: Dict[str, Any] = {"code": code, "message": message}
		if details is not None:
			err["details"] = details
		return {"errors": [err]}
