
select id1, time1, time2, cast ("2015-01-01T10:45:00.234" as timestamp)
from Foo
where time1 = cast("2014-05-05T10:45:00.234" as timestamp)
