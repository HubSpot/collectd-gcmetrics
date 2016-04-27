#!/usr/bin/python
import os, re

# metric names:
eden_avg    = 'gauge.g1gc.eden.size'
tenured_avg = 'gauge.g1gc.tenured.size'
threshold   = 'gauge.g1gc.tenured.threshold'
mixed_count = 'counter.g1gc.mixedgc.count'
mixed_total = 'counter.g1gc.mixedgc.time'
young_count = 'counter.g1gc.younggc.count'
young_total = 'counter.g1gc.younggc.time'
full_count  = 'counter.g1gc.fullgc.count'
full_total  = 'counter.g1gc.fullgc.time'
max_pause   = 'gauge.g1gc.pause.time.max'
long_pause  = 'counter.g1gc.longpause.count'
humongous   = 'counter.g1gc.humongous.count'

# G1GC log regexes:
before_after = '([0-9\.]+[BKMG])->([0-9\.]+[BKMG])'
before_after_wcap = '([0-9\.]+[BKMG])\(([0-9\.]+[BKMG])\)->([0-9\.]+[BKMG])\(([0-9\.]+[BKMG])\)'

heap_pat = re.compile('\s*\[Eden: %s Survivors: %s Heap: %s' % (before_after_wcap, before_after, before_after_wcap))
threshold_pat = re.compile('.*threshold: ([0-9]+) bytes .*, source: end of GC\]') # only need most recent
gc_start_pat = re.compile('[0-9T\-\:\.\+]* [0-9\.]*: \[GC pause \(G1 Evacuation Pause\) \((young|mixed)\)') # use previous occurrence of this to track pause types
full_gc_pat = re.compile('[0-9T\-\:\.\+]* [0-9\.]*: \[Full GC ')
pause_pat = re.compile('\s*\[Times: user=[0-9\.]+ sys=[0-9\.]+, real=([0-9\.]+) secs')
humongous_pat = re.compile('.* source: concurrent humongous allocation\]$')

# groups for heap pat:
before_eden_sz, before_eden_cap, after_eden_sz, after_eden_cap, before_survivor, after_survivor, before_heap_sz, before_heap_cap, after_heap_sz, after_heap_cap = range(1,11)

# memory string conversion to bytes:
mem_pat = re.compile('([0-9\.]+)([BKMG])')
mem_factors = { 'B':0, 'K':1, 'M':2, 'G':3 }

def _to_bytes(mem_amt):
  match = mem_pat.match(mem_amt)
  if match:
    return int(float(match.group(1)) * 1024 ** mem_factors[match.group(2)])
  else:
    return -1  # error

def _mean_rounded(values):
  return int(round(float(sum(values))/len(values))) if values else 0

# signalfx-formatted family metric prefix:
def _family_qual(family):
  return family.lower().strip('. \t\n\r').replace(' ', '_') if family else ''

class G1GCMetrics(object):
  def __init__(self, collectd, logdir=None, log_prefix="gc", family=None, eden=True, tenured=True, ihop_threshold=True, mixed_pause=True, young_pause=True, full_pause=True, pause_max=True, pause_threshold=None, humongous_enabled=True, verbose=False):
    self.collectd = collectd
    self.logdir = logdir
    self.log_prefix = log_prefix
    self.family = _family_qual(family)
    self.eden = eden
    self.tenured = tenured
    self.ihop_threshold = ihop_threshold
    self.mixed_pause = mixed_pause
    self.young_pause = young_pause
    self.full_pause = full_pause
    self.pause_max = pause_max
    self.pause_threshold = pause_threshold
    self.humongous_enabled = humongous_enabled
    self.verbose = verbose

    self.prev_log = None
    self.log_seek = 0
    self.prev_gc_type = None
    self.current_metrics = {
        eden_avg    : None,
        tenured_avg : None,
        threshold   : None,
        mixed_count : 0,
        mixed_total : 0,
        young_count : 0,
        young_total : 0,
        full_count  : 0,
        full_total  : 0,
        max_pause   : None,
        long_pause  : 0,
        humongous   : 0
        }

  def configure_callback(self, conf):
    """called by collectd to configure the plugin. This is called only once"""
    for node in conf.children:
      if node.key == 'LogDir':
        self.logdir = node.values[0]
      elif node.key == 'LogPrefix':
        self.log_prefix = node.values[0]
      elif node.key == 'Family':
        self.family = _family_qual(node.values[0])
      elif node.key == 'MeasureEdenAvg':
        self.eden = bool(node.values[0])
      elif node.key == 'MeasureTenuredAvg':
        self.tenured = bool(node.values[0])
      elif node.key == 'MeasureIHOPThreshold':
        self.ihop_threshold = bool(node.values[0])
      elif node.key == 'MeasureMixedPause':
        self.mixed_pause = bool(node.values[0])
      elif node.key == 'MeasureYoungPause':
        self.young_pause = bool(node.values[0])
      elif node.key == 'MeasureFullPause':
        self.full_pause = bool(node.values[0])
      elif node.key == 'MeasureMaxPause':
        self.pause_max = bool(node.values[0])
      elif node.key == 'LongPauseThreshold':
        self.pause_threshold = int(node.values[0])
      elif node.key == 'CountHumongousObjects':
        self.humongous_enabled = bool(node.values[0])
      elif node.key == 'Verbose':
        self.verbose = bool(node.values[0])
      else:
        self.collectd.warning('g1gc plugin: Unknown config key: %s.' % (node.key))
    self.current_metrics = {
        eden_avg    : None,
        tenured_avg : None,
        threshold   : None,
        mixed_count : 0 if self.mixed_pause else None,
        mixed_total : 0 if self.mixed_pause else None,
        young_count : 0 if self.young_pause else None,
        young_total : 0 if self.young_pause else None,
        full_count  : 0 if self.full_pause else None,
        full_total  : 0 if self.full_pause else None,
        max_pause   : None,
        long_pause  : 0 if self.pause_threshold else None,
        humongous   : 0 if self.humongous_enabled else None
        }

  def read_callback(self):
    """read the most-recently modified GC log in logdir, then return most recent datapoints from it"""
    if self.logdir:
      gc_logs = sorted([self.logdir + os.sep + log for log in os.listdir(self.logdir) if log.startswith(self.log_prefix)], key=os.path.getmtime)
      if gc_logs:
        new_metrics = self.read_recent_data_from_log(gc_logs[-1])
        if new_metrics:
          self.dispatch_metrics(self.update_metrics(new_metrics))
    else:
      self.collectd.warning('g1gc plugin: skipping because no log directory ("LogDir") has been configured')

  def read_recent_data_from_log(self, logpath):
    is_first_run = self.prev_log == None
    if logpath != self.prev_log:
      self.reset_log(logpath)
    gc_lines = []
    f = open(logpath)
    try:
      f.seek(self.log_seek)
      gc_lines = f.readlines()
      self.log_seek = f.tell()
    finally:
      f.close()
    if is_first_run:
      # don't process full log (may have restarted recently, don't want to double-count
      # instead, just run through the existing GC log, find type of the last GC (for next run)
      self.find_last_gc_type(gc_lines)
      self.log_verbose("skipping metrics dispatch, since this is the first read since plugin restart")
      return {}
    # else, read metrics from logs as usual
    edens = []
    tenures = []
    threshold_bytes = 0
    mixed_pauses = []
    young_pauses = []
    full_pauses = []
    humongous_count = 0
    match = None
    for line in gc_lines:
      if self.eden or self.tenured:
        match = heap_pat.match(line)
        if match:
          young_sz = _to_bytes(match.group(after_eden_cap)) + _to_bytes(match.group(after_survivor))
          if self.eden:
            self.log_verbose("recording Eden size of %d bytes" % young_sz)
            edens.append(young_sz)
          if self.tenured:
            old_sz = _to_bytes(match.group(after_heap_sz)) - _to_bytes(match.group(after_survivor))
            self.log_verbose("recording Tenured size of %d bytes" % old_sz)
            tenures.append(old_sz)
          continue
      if self.ihop_threshold:
        match = threshold_pat.match(line)
        if match:
          threshold_bytes = int(match.group(1))
          self.log_verbose("recording tenured space threshold of %d bytes" % threshold_bytes)
          continue
      if self.mixed_pause or self.young_pause or self.full_pause:
        match = gc_start_pat.match(line)
        if match:
          self.prev_gc_type = match.group(1)
          continue
        match = full_gc_pat.match(line)
        if match:
          self.prev_gc_type = "full"
          continue
      if self.any_pause_metrics_enabled():
        match = pause_pat.match(line)
        if match:
          pause_ms = int(round(float(match.group(1)) * 1000))
          self.log_verbose("recording %d ms pause of type %s" % (pause_ms, self.prev_gc_type))
          if self.prev_gc_type == 'mixed':
            mixed_pauses.append(pause_ms)
          elif self.prev_gc_type == 'young':
            young_pauses.append(pause_ms)
          else:
            full_pauses.append(pause_ms)
          continue
      if self.humongous_enabled:
        match = humongous_pat.match(line)
        if match:
          humongous_count += 1
          continue
    metrics = { 
        eden_avg    : _mean_rounded(edens) if (self.eden and edens) else None,
        tenured_avg : _mean_rounded(tenures) if (self.tenured and tenures) else None,
        threshold   : threshold_bytes if (self.ihop_threshold and threshold_bytes) else None,
        mixed_count : len(mixed_pauses) if self.mixed_pause else None,
        mixed_total : sum(mixed_pauses) if self.mixed_pause else None,
        young_count : len(young_pauses) if self.young_pause else None,
        young_total : sum(young_pauses) if self.young_pause else None,
        full_count  : len(full_pauses) if self.full_pause else None,
        full_total  : sum(full_pauses) if self.full_pause else None,
        max_pause   : max(mixed_pauses + young_pauses + full_pauses if mixed_pauses or young_pauses or full_pauses else [0]) if self.pause_max else None,
        long_pause  : len(filter(lambda y: y > self.pause_threshold, mixed_pauses + young_pauses + full_pauses)) if self.pause_threshold else None,
        humongous   : humongous_count if self.humongous_enabled else None
        }
    return metrics

  def reset_log(self, logpath):
    self.prev_log = logpath
    self.log_seek = 0
    self.prev_gc_type = None

  def any_pause_metrics_enabled(self):
    return self.mixed_pause or self.young_pause or self.full_pause or self.pause_max or self.pause_threshold

  def find_last_gc_type(self, gc_lines):
    for line in gc_lines:
      match = gc_start_pat.match(line)
      if match:
        self.prev_gc_type = match.group(1)
      else:
        match = full_gc_pat.match(line)
        if match:
          self.prev_gc_type = "full"

  def update_metrics(self, new_metrics):
    """updates metrics from last run with new metrics, to make current"""
    for metric in self.current_metrics:
      new_value = new_metrics[metric] if metric in new_metrics else None
      if metric.startswith('counter') and new_value:
        self.current_metrics[metric] += new_value
      elif metric.startswith('gauge'):
        self.current_metrics[metric] = new_value
    return self.current_metrics

  def dispatch_metrics(self, metrics):
    for metric in metrics:
      value = metrics[metric]
      if value == None:
        continue
      self.log_verbose('Sending value %s %s=%s' % (self.family, metric, value))
      
      data_type, type_instance = metric.split(".", 1)
      val = self.collectd.Values(plugin='g1gc')
      val.type = data_type
      val.type_instance = type_instance
      val.plugin_instance = self.family
      val.values = [value]
      val.dispatch()

  def log_verbose(self, msg):
    if self.verbose:
      self.collectd.info('g1gc plugin [verbose]: '+msg)

# The following classes are copied from collectd-mapreduce/mapreduce_utils.py
# to launch the plugin manually (./g1gcmetrics.py) for development
# purposes. They basically mock the calls on the "collectd" symbol
# so everything prints to stdout.
class CollectdMock(object):

  def __init__(self, plugin):
    self.value_mock = CollectdValuesMock
    self.plugin = plugin

  def info(self, msg):
    print 'INFO: %s' % (msg)

  def warning(self, msg):
    print 'WARN: %s' % (msg)

  def error(self, msg):
    print 'ERROR: %s' % (msg)
    sys.exit(1)

  def Values(self, plugin=None):
    return (self.value_mock)()

class CollectdValuesMock(object):

  def dispatch(self):
        print self

  def __str__(self):
    attrs = []
    for name in dir(self):
      if not name.startswith('_') and name is not 'dispatch':
        attrs.append("%s=%s" % (name, getattr(self, name)))
    return "<CollectdValues %s>" % (' '.join(attrs))

if __name__ == '__main__':
  from time import sleep
  collectd = CollectdMock('g1gc')
  gc = G1GCMetrics(collectd, logdir='/tmp/logs/', pause_threshold=1000, verbose=True)
  gc.read_callback()
  for i in range (0,2):
    sleep(60)
    gc.read_callback()
else:
  import collectd
  gc = G1GCMetrics(collectd)
  collectd.register_config(gc.configure_callback)
  collectd.register_read(gc.read_callback)
