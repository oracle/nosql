select id1, time1
from Foo
where time1 = cast("2014-05-05T10:45:00.0" as timestamp(3))
