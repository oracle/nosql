select count(*)
from Foo f
where f.record.long > 10
order by count(*)
