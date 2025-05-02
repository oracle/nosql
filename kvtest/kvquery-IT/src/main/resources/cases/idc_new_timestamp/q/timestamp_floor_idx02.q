select id, timestamp_floor(t3,'hour') as t3_to_hour
from roundFunc
where timestamp_floor(t3,'hour') > '2020-02-28T23:00:00'