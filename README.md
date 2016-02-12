# Garbage Collection metrics CollectD plugin

A [CollectD](http://collectd.org) plugin to collect G1GC stats from a local JVM. Uses CollectD's [Python plugin](http://collectd.org/documentation/manpages/collectd-python.5.shtml).
Size and time metric units are bytes and milliseconds, respectively.

####Configuration parameters (defaults refer to values in g1gcstats.conf):
- **`LogDir`**: directory to find GC logs in (REQUIRED: no default).
- **`Family`**: qualifier, name for the cluster type the plugin is running on (`""`).
- **`Verbose`**: if `true`, print verbose logging (`false`).
- **`MeasureEdenAvg`**: if `true`, record and send mean Eden size (`true`).
- **`MeasureTenuredAvg`**: if `true`, record and send mean Tenured (Old Gen) size (`true`).
- **`MeasureIHOPThreshold`**: if `true`, record and send Old Gen mixed GC threshold (`true`).
- **`MeasureMixedPause`**: if `true`, record and send mixed GC count and pause time (`true`).
- **`MeasureYoungPause`**: if `true`, record and send young GC count and pause time (`true`).
- **`MeasureMaxPause`**: if `true`, record and send longest GC pause since last report (`true`).
- **`LongPauseThreshold`**: (integer) if non-zero, count pauses longer than config ms (`1000`).
