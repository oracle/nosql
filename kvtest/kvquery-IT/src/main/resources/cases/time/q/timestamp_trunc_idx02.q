select id
from roundtest t
where timestamp_trunc(t.s9, 'day') = cast('2021-02-28' as timestamp)

