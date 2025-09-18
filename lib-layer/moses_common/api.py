# print("Loaded API module")

import json
import re
import urllib.parse
from typing import Dict, Any, Tuple, Optional, List, NamedTuple

import moses_common.__init__ as common
import moses_common.dynamodb
import moses_common.ui


class RouteNotFound(Exception):
	pass

class MethodNotAllowed(Exception):
	def __init__(self, allowed):
		super().__init__(f"Method not allowed. Allowed: {', '.join(sorted(allowed))}")
		self.allowed = set(allowed)

def _normalize_path(p: str) -> str:
	if not p.startswith("/"):
		p = "/" + p
	if len(p) > 1 and p.endswith("/"):
		p = p.rstrip("/")
	while "//" in p:
		p = p.replace("//", "/")
	return p

def _split_segments(p: str) -> List[str]:
	p = _normalize_path(p)
	return [urllib.parse.unquote(seg) for seg in p.split("/") if seg]

def _is_var(seg: str) -> bool:
	return seg.startswith("{") and seg.endswith("}") and len(seg) > 2

def _is_greedy(seg: str) -> bool:
	return _is_var(seg) and seg.endswith("*}")

def _var_name(seg: str) -> str:
	core = seg[1:-1]
	return core[:-1] if core.endswith("*") else core

class _CompiledRoute(NamedTuple):
	route_key: str
	segments: List[str]
	methods: Dict[str, Any]		 # method (UPPER) -> action
	literal_positions: int
	has_greedy: bool

def _compile_schema(schema: Dict[str, Dict[str, Any]]) -> List[_CompiledRoute]:
	compiled: List[_CompiledRoute] = []
	for route, methods in schema.items():
		segs = _split_segments(route)
		has_greedy = any(_is_greedy(s) for s in segs)
		if has_greedy and (len(segs) == 0 or not _is_greedy(segs[-1])):
			raise ValueError(f"Greedy parameter must be the final segment: {route}")
		literal_positions = sum(1 for s in segs if not _is_var(s))
		methods_up = {m.upper(): v for m, v in methods.items()}
		compiled.append(_CompiledRoute(route, segs, methods_up, literal_positions, has_greedy))
	compiled.sort(key=lambda r: (r.literal_positions, len(r.segments), not r.has_greedy), reverse=True)
	return compiled

def _match_against(comp: _CompiledRoute, req_segments: List[str]) -> Optional[Tuple[Dict[str, str], int, int]]:
	# returns (path_vars, literal_score, greedy_len) or None
	rsegs = comp.segments
	path_vars: Dict[str, str] = {}

	if comp.has_greedy:
		fixed = rsegs[:-1]
		if len(req_segments) < len(fixed):
			return None
		literal_score = 0
		for r_seg, p_seg in zip(fixed, req_segments[:len(fixed)]):
			if _is_var(r_seg):
				name = _var_name(r_seg)
				if name in path_vars:
					return None
				path_vars[name] = p_seg
			else:
				if r_seg != p_seg:
					return None
				literal_score += 1
		gname = _var_name(rsegs[-1])
		rest = req_segments[len(fixed):]
		path_vars[gname] = "/".join(rest)
		return path_vars, literal_score, len(rest)
	else:
		if len(rsegs) != len(req_segments):
			return None
		literal_score = 0
		for r_seg, p_seg in zip(rsegs, req_segments):
			if _is_var(r_seg):
				name = _var_name(r_seg)
				if name in path_vars:
					return None
				path_vars[name] = p_seg
			else:
				if r_seg != p_seg:
					return None
				literal_score += 1
		return path_vars, literal_score, 0


class API:
	"""
	Usage:
		api = API(api_schema, api_gateway, path_prefix='/api/v1', ui=None, dry_run=False)
		result = api.route()
		# -> {"action": {...}, "path_vars": {...}}
	"""
	def __init__(self,
				 api_schema: Dict[str, Dict[str, Any]],
				 api_gateway,
				 *,
				 path: str = "/",
				 method: str = "GET",
				 path_prefix: Optional[str] = None,
				 precompile: bool = True,
				 strict_prefix: bool = False,
				 head_fallback_to_get: bool = False,
				 ui = None,
				 dry_run: bool = False):
		self.dry_run = dry_run
		self.ui = ui or moses_common.ui.Interface()
		
		self.api_schema = api_schema
		self.api_gateway = api_gateway
		self.path_prefix = path_prefix
		self.strict_prefix = strict_prefix
		self.head_fallback_to_get = head_fallback_to_get
		self._compiled = _compile_schema(api_schema) if precompile else None
		
		self.path = self.api_gateway.path
		self.method = self.api_gateway.method
		

	def set_request(self, *, path: str, method: str) -> None:
		self.path = path
		self.method = method

	@staticmethod
	def _strip_prefix(path: str, prefix: Optional[str], strict: bool) -> str:
		if not prefix:
			return _normalize_path(path)
		prefix_n = _normalize_path(prefix)
		path_n = _normalize_path(path)
		if path_n.startswith(prefix_n):
			trimmed = path_n[len(prefix_n):]
			if not trimmed:
				return "/"
			return trimmed if trimmed.startswith("/") else "/" + trimmed
		if strict:
			raise RouteNotFound(f"Path '{path}' does not start with required prefix '{prefix}'")
		return path_n

	def _find_match(self, req_segments: List[str]) -> _CompiledRoute | None | Tuple[_CompiledRoute, Dict[str, str]]:
		compiled = self._compiled or _compile_schema(self.api_schema)
		best = None	 # (comp, path_vars, literal_score, greedy_len, seg_len)
		for comp in compiled:
			m = _match_against(comp, req_segments)
			if not m:
				continue
			path_vars, literal_score, greedy_len = m
			cand = (comp, path_vars, literal_score, greedy_len, len(comp.segments))
			if best is None:
				best = cand
			else:
				_, _, ls_b, gl_b, sl_b = best
				# tie-break: higher literals > longer route > fewer greedy segs > prefer non-greedy
				if (literal_score, len(comp.segments), -greedy_len, not comp.has_greedy) > (ls_b, sl_b, -gl_b, not best[0].has_greedy):
					best = cand
		if best is None:
			return None
		return best[0], best[1]

	def route(self) -> Dict[str, Any]:
		method = self.method.upper()
		# optional HEAD -> GET fallback (common in APIs)
		method_alt = "GET" if (method == "HEAD" and self.head_fallback_to_get) else None

		path = self._strip_prefix(self.path, self.path_prefix, self.strict_prefix)
		req_segments = _split_segments(path)

		found = self._find_match(req_segments)
		if not found:
			raise RouteNotFound(f"No route matched '{path}'")
		comp, path_vars = found

		if method in comp.methods:
			return {"action": comp.methods[method], "method": method, "path_vars": path_vars}
		if method_alt and method_alt in comp.methods:
			return {"action": comp.methods[method_alt], "method": method, "path_vars": path_vars}
		raise MethodNotAllowed(comp.methods.keys())
	
	
	def act(self, action):
		if action['source'] == 'ddb':
			return self.ddb_action(action)
		else:
			return False, f"Source '{action['source']}' is not supported"
		
	
	def ddb_action(self, action):
		table = moses_common.dynamodb.Table(action['table'], ui=self.ui, dry_run=self.dry_run)
		if action['method'] == 'GET':
			pass
		elif action['method'] == 'POST':
			pass
		elif action['method'] == 'PATCH':
			pass
		elif action['method'] == 'DELETE':
			pass
