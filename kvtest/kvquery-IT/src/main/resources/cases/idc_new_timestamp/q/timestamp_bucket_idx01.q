select id, timestamp_bucket(t3,'7 days') as t3_to_7_days
from roundFunc
where timestamp_bucket(t3,'7 days') >= '2021-01-01T00:00:00'