#Testcase for invalid test result

select
t0, timestamp_trunc(t0, "iyear"),
t3, timestamp_trunc(t3),
s9, timestamp_trunc(s9)
from roundFunc where id=2