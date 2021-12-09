#!/usr/bin/python
import os, re
import argparse

MEGABYTES = 1024*1024

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


class Parser(object):
  def __init__(self, config):
    self.config = config
    self.prev_gc_type = config.prev_gc_type
    self.edens = []
    self.tenures = []
    self.threshold_bytes = 0
    self.mixed_pauses = []
    self.young_pauses = []
    self.full_pauses = []
    self.humongous_count = 0

  def parse_heap(self, line):
      raise NotImplementedError('parse_heap not implemented')

  def parse_ihop_threshold(self, line):
    raise NotImplementedError('parse_ihop_threshold not implemented')

  def parse_gc_start(self, line):
    raise NotImplementedError('parse_gc_start not implemented')

  def parse_pause_time(self, line):
    raise NotImplementedError('parse_pause_time not implemented')

  def parse_humongous_alloc(self, line):
    raise NotImplementedError('parse_humongous_alloc not implemented')

  def test(self, lines):
    for line in lines:
      if self.parse_gc_start(line):
        return True
    return False

  def find_last_gc_type(self, lines):
    for line in lines:
      result = self.parse_gc_start(line)
      if result:
        self.prev_gc_type = result

  def parse(self, line):
    if self.config.eden or self.config.tenured:
      result = self.parse_heap(line)
      if result:
        if self.config.eden:
          self.config.log_verbose("recording Eden size of %d bytes" % result['young'])
          self.edens.append(result['young'])
        if self.config.tenured:
          self.config.log_verbose("recording Tenured size of %d bytes" % result['old'])
          self.tenures.append(result['old'])
        return

    if self.config.ihop_threshold:
      result = self.parse_ihop_threshold(line)
      if result is not None:
        self.threshold_bytes = result
        self.config.log_verbose("recording tenured space threshold of %d bytes" % self.threshold_bytes)
        return

    if self.config.any_pause_metrics_enabled():
      result = self.parse_gc_start(line)
      if result:
        self.prev_gc_type = result

      # assumes that timings come after the gc start line
      result = self.parse_pause_time(line)
      if result is not None:
        if self.prev_gc_type == 'mixed':
          self.mixed_pauses.append(result)
        elif self.prev_gc_type == 'young':
          self.young_pauses.append(result)
        else:
          self.full_pauses.append(result)
        return

    if self.config.humongous_enabled:
      result = self.parse_humongous_alloc(line)
      if result:
        self.humongous_count += 1

  def to_metrics(self):
    if self.config.pause_max:
      max_pause_val = max([0] + self.mixed_pauses + self.young_pauses + self.full_pauses)
    else:
      max_pause_val = None

    if self.config.pause_threshold:
      long_pause_val = len(filter(lambda y: y > self.config.pause_threshold, self.mixed_pauses + self.young_pauses + self.full_pauses))
    else:
      long_pause_val = None

    return {
        eden_avg    : _mean_rounded(self.edens) if (self.config.eden and self.edens) else None,
        tenured_avg : _mean_rounded(self.tenures) if (self.config.tenured and self.tenures) else None,
        threshold   : self.threshold_bytes if (self.config.ihop_threshold and self.threshold_bytes) else None,
        mixed_count : len(self.mixed_pauses) if self.config.mixed_pause else None,
        mixed_total : sum(self.mixed_pauses) if self.config.mixed_pause else None,
        young_count : len(self.young_pauses) if self.config.young_pause else None,
        young_total : sum(self.young_pauses) if self.config.young_pause else None,
        full_count  : len(self.full_pauses) if self.config.full_pause else None,
        full_total  : sum(self.full_pauses) if self.config.full_pause else None,
        max_pause   : max_pause_val,
        long_pause  : long_pause_val,
        humongous   : self.humongous_count if self.config.humongous_enabled else None
    }


class Java8Parser(Parser):
  before_after = '([0-9\.]+[BKMG])->([0-9\.]+[BKMG])'
  before_after_wcap = '([0-9\.]+[BKMG])\(([0-9\.]+[BKMG])\)->([0-9\.]+[BKMG])\(([0-9\.]+[BKMG])\)'
  heap_pat = re.compile('\s*\[Eden: %s Survivors: %s Heap: %s' % (before_after_wcap, before_after, before_after_wcap))
  before_eden_sz, before_eden_cap, after_eden_sz, after_eden_cap, before_survivor, after_survivor, before_heap_sz, before_heap_cap, after_heap_sz, after_heap_cap = range(1,11)

  threshold_pat = re.compile('.*threshold: ([0-9]+) bytes .*, source: end of GC\]') # only need most recent
  gc_start_pat = re.compile('[0-9T\-\:\.\+]* [0-9\.]*: \[GC pause \(G1 Evacuation Pause\) \((young|mixed)\)') # use previous occurrence of this to track pause types
  full_gc_pat = re.compile('[0-9T\-\:\.\+]* [0-9\.]*: \[Full GC ')
  pause_pat = re.compile('\s*\[Times: user=[0-9\.]+ sys=[0-9\.]+, real=([0-9\.]+) secs')
  humongous_pat = re.compile('.* source: concurrent humongous allocation\]$')

  def __init__(self, config):
    super(Java8Parser, self).__init__(config)

  def parse_heap(self, line):
      match = self.heap_pat.match(line)
      if match:
        young_sz = _to_bytes(match.group(self.after_eden_cap)) + _to_bytes(match.group(self.after_survivor))
        old_sz = _to_bytes(match.group(self.after_heap_sz)) - _to_bytes(match.group(self.after_survivor))
        return {'young': young_sz, 'old': old_sz}
      return None

  def parse_ihop_threshold(self, line):
    match = self.threshold_pat.match(line)
    if match:
      return int(match.group(1))
    return None

  def parse_gc_start(self, line):
    match = self.gc_start_pat.match(line)
    if match:
      return match.group(1)
    match = self.full_gc_pat.match(line)
    if match:
      return "full"
    return None

  def parse_pause_time(self, line):
    match = self.pause_pat.match(line)
    if match:
      return int(round(float(match.group(1)) * 1000))
    return None

  def parse_humongous_alloc(self, line):
    return self.humongous_pat.match(line)


class Java9PlusParser(Parser):
  regions_pat = re.compile(r'.*GC\(\d+\) (Eden|Survivor|Old|Humongous) regions: (\d+)->(\d+)(?:\((\d+)\))?')
  threshold_pat = re.compile(r'.*GC\(\d+\) .*threshold: ([0-9]+)B \([\d\.]+\) source: end of GC') # only need most recent
  gc_start_pat = re.compile(r'.*GC\(\d+\) Pause (Young|Mixed|Full)')
  pause_pat = re.compile(r'.*GC\(\d+\) User=[0-9\.]+s Sys=[0-9\.]+s Real=([0-9\.]+)s')
  humongous_pat = re.compile(r'.*GC\(\d+\) .* source: concurrent humongous allocation$')

  def __init__(self, config):
    super(Java9PlusParser, self).__init__(config)

    # used to collect multi-line heap sizing metrics below, will be reset after each block
    self.young_regions = 0

  def parse_heap(self, line):
      match = self.regions_pat.match(line)
      if match:
        region_type = match.group(1)

        if region_type == 'Eden' or region_type == 'Survivor':
          # we add post_gc cap for eden and survivor together to get total young size
          # they come one after another on separate lines
          self.young_regions += int(match.group(4))
        elif region_type == 'Old':
          # after eden/survivor comes old, and here we can report the full result
          young_sz = self.config.region_size_mb * MEGABYTES * self.young_regions

          # old doesn't have a cap, so just use post-gc amount
          old_sz = self.config.region_size_mb * MEGABYTES * int(match.group(3))

          # reset young_regions for next gc
          self.young_regions = 0

          return {'young': young_sz, 'old': old_sz}

      return None

  def parse_ihop_threshold(self, line):
    match = self.threshold_pat.match(line)
    if match:
      return int(match.group(1))
    return None

  def parse_gc_start(self, line):
    match = self.gc_start_pat.match(line)
    if match:
      return match.group(1).lower()
    return None

  def parse_pause_time(self, line):
    match = self.pause_pat.match(line)
    if match:
      return int(round(float(match.group(1)) * 1000))
    return None

  def parse_humongous_alloc(self, line):
    return self.humongous_pat.match(line)


class LogHandle(object):
  def __init__(self, log_path):
    self.log_path = log_path
    stat = os.stat(log_path)
    self.inode = stat.st_ino
    self.size = stat.st_size

  def is_new_file(self, other):
    # handled cases: file name changes, inode changes, or file seems to have been truncated
    return self.log_path != other.log_path or self.inode != other.inode or self.size < other.size

  def fetch_from(self, offset):
    with open(self.log_path, 'r') as f:
      f.seek(offset)
      return f.readlines(), f.tell()

  def __repr__(self):
      return 'path=%s, inode=%s, size=%s' % (self.log_path, self.inode, self.size)


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
  def __init__(self, collectd, logdir=None, log_prefix="gc", family=None, region_size_mb=1, eden=True, tenured=True, ihop_threshold=True, mixed_pause=True, young_pause=True, full_pause=True, pause_max=True, pause_threshold=None, humongous_enabled=True, verbose=False, skip_first=True):
    self.collectd = collectd
    self.logdir = logdir
    self.log_prefix = log_prefix
    self.family = _family_qual(family)
    self.region_size_mb = region_size_mb
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
    self.skip_first = skip_first

    self.parser = None
    self.prev_log = None
    self.log_offset = 0
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
      elif node.key == 'RegionSizeMB':
        self.region_size_mb = int(node.values[0])
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
        new_metrics = self.read_recent_data_from_log(LogHandle(gc_logs[-1]))
        if new_metrics:
          self.dispatch_metrics(self.update_metrics(new_metrics))
    else:
      self.collectd.warning('g1gc plugin: skipping because no log directory ("LogDir") has been configured')

  def get_parser(self, gc_lines):
    if self.parser:
      return self.parser

    parser = Java8Parser(self)
    if parser.test(gc_lines):
      self.collectd.info("g1gc plugin: using Java8 gc log parser")
      self.parser = parser
      return self.parser

    parser = Java9PlusParser(self)
    if parser.test(gc_lines):
      self.collectd.info("g1gc plugin: using Java9+ gc log parser")
      self.parser = parser
      return self.parser

    self.collectd.warning('g1gc plugin: could not detect appropriate parser based on current content of gc logs, will try again at next interval')
    return None

  def read_recent_data_from_log(self, log_handle):
    is_first_run = self.prev_log == None

    if is_first_run or log_handle.is_new_file(self.prev_log):
      self.collectd.info('g1gc plugin: reading new log file: %s' % log_handle)
      self.reset_log(log_handle)

    # always update prev_log, so we keep the latest size
    self.prev_log = log_handle

    gc_lines, new_offset = log_handle.fetch_from(self.log_offset)
    self.log_offset = new_offset

    parser = self.get_parser(gc_lines)
    if not parser:
      return {}

    try:
      if is_first_run and self.skip_first:
        # don't process full log (may have restarted recently, don't want to double-count
        # instead, just run through the existing GC log, find type of the last GC (for next run)
        parser.find_last_gc_type(gc_lines)
        self.log_verbose("skipping metrics dispatch, since this is the first read since plugin restart")
        return {}

      # else, read metrics from logs as usual
      for line in gc_lines:
        parser.parse(line)

      return parser.to_metrics()
    finally:
      if parser.prev_gc_type:
        self.prev_gc_type = parser.prev_gc_type

  def reset_log(self, log_handle):
    self.prev_log = log_handle
    self.log_offset = 0
    self.prev_gc_type = None

  def any_pause_metrics_enabled(self):
    return self.mixed_pause or self.young_pause or self.full_pause or self.pause_max or self.pause_threshold

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
  parser = argparse.ArgumentParser(description='Parse gc logs into collectd metrics - debug mode')
  parser.add_argument('--log-dir', '-d', metavar='PATH', default='/tmp/logs', help='path to directory with gc logs in them')
  args = parser.parse_args()

  from time import sleep
  collectd = CollectdMock('g1gc')
  gc = G1GCMetrics(collectd, logdir=args.log_dir, pause_threshold=1000, verbose=True, skip_first=False)
  gc.read_callback()
  for i in range (0,2):
    sleep(60)
    gc.read_callback()
else:
  import collectd
  gc = G1GCMetrics(collectd)
  collectd.register_config(gc.configure_callback)
  collectd.register_read(gc.read_callback)
