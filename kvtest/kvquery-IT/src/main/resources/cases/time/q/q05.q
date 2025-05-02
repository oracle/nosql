select id1, time1, cast(time1 as timestamp(1)), cast(time2 as timestamp(3))
from Foo
order by id1
