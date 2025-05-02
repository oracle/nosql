#Testcase for timestamp or pattern is null
select
t0,format_timestamp(t0,"yyyy-MM-dd"),
s3,format_timestamp(s3,t0)
from roundFunc where id=1