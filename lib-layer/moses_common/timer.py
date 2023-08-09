# print("Loaded Timer module")

import inspect
import datetime
import time

import moses_common.__init__ as common


"""
import moses_common.timer
"""

class TimerSet:
	"""
	timers = moses_common.timer.TimerSet()
	"""
	def __init__(self):
		self._timer_list = []
		self._base_depth = len(inspect.stack())
		self.add('full_set')
	
	@property
	def last_timer(self):
		if len(self._timer_list) > 1:
			return self._timer_list[-1]
		return None
	
	@property
	def timers(self):
		return self._timer_list
	
	
	def add(self, label, depth=None):
		cdepth = len(inspect.stack()) - self._base_depth
		if depth:
			cdepth = depth
		timer = Timer(label, cdepth)
		self._timer_list.append(timer)
		return timer.start_time
		
	def stop(self, label=None):
		timer = None
		if label:
			timer = self.get_timer(label)
			if not timer:
				raise ValueError("Timer not found:", label)
		else:
			timer = self.last_timer
			if not timer:
				raise ValueError("No timers found")
		return timer.stop()
	
	def end(self):
		return self.stop('full_set')
	
	def get_timer(self, label):
		for i in range(len(self._timer_list), 0, -1):
			if self._timer_list[i-1].label == label:
				return self._timer_list[i-1]
	
	def as_string(self, args=None):
		output = []
		for timer in self._timer_list:
			output.append(timer.as_string(args))
		return "\n".join(output)
	

class Timer:
	"""
	timer = moses_common.timer.Timer()
	"""
	def __init__(self, label, depth=None):
		self._label = label
		self._start_time = time.time()
		self._end_time = None
		self._depth = len(inspect.stack())
		if depth:
			self._depth = depth
	
	def __repr__(self):
		if self._depth:
			return f"Timer('{self.label}', {self._depth})"
		return f"Timer('{self.label}')"
	
	"""
	print(timer)
	"""
	def __str__(self):
		if self.duration:
			return 'Timer: {}s {}'.format(str(round(self.duration, 2)), self.label)
		return f"Timer: running {self.label}"
	
	@property
	def label(self):
		return self._label
	
	@property
	def start_time(self):
		return self._start_time
	
	@property
	def end_time(self):
		return self._end_time
	
	@property
	def duration(self):
		if self._end_time:
			return self._end_time - self._start_time
		return None
	
	"""
	timer.stop()
	"""
	def stop(self):
		self._end_time = time.time()
		return str(self)
	
	def as_string(self, args=None):
		indent = ''
		if args:
			if 'indent' in args:
				indent = ' '*int(args['indent'])
		indent += '  '*self._depth
		dur_string = 'running'
		if self.duration:
			dur_string = str(round(self.duration, 2)) + 's'
		
# 		dur_string = dur_string.rjust(7, ' ')
		return "{}{}: {}".format(indent, dur_string, self.label)


class Refresh:
	"""
	timer = moses_common.timer.Refresh()
	"""
	def __init__(self):
		self.start_time = common.get_dt_now()
	
	"""
	if timer.refresh(minutes=5):
		# do stuff
	"""
	def refresh(self, minutes=5):
		now = common.get_dt_now()
		if now > self.start_time + datetime.timedelta(minutes=minutes):
			self.start_time = now
			return True
		return False

