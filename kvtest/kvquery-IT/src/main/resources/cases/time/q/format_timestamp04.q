# Return null if input is not castable to timestamp

select format_timestamp(t.info.dt.pattern) as d
from arithtest t
where id = 0
