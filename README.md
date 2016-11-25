WARNING: This is only an experimental tool and is not meant to be used in production.

# Metricbeat Curator

The Metricbeat curator was created to reduce the number of events generated by Metricbeat. Assuming Metricbeat collects data every 5 seconds but for older data only a period of 30 seconds is need, the curator can reduce the number of events to this period.

# How it works

Metricbeat curator DOES NOT do any aggregation of the events. All it does is remove events between to periods. If events are collected every 5 seconds and the new period is set to 15s, it will be as if events would have been collected every 15 seconds as the events for second 0 and 15 are kept, but 5 and 10 are removed.

In case the collecting frequency was set to 20s and is curated with 30s, the new frequency will be 40s because one intermediate event will always be removed which leads to a 40s period.


# TODO

* Add option to filter by module and metricset for events which should be curated -> add to query
* Add support for older_then param to only clean up old messages