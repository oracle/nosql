#Testcase for invalid test result

select
t0, timestamp_floor(t0, "iyear"),
t3, timestamp_floor(t3),
s9, timestamp_floor(s9)
from roundFunc where id=2