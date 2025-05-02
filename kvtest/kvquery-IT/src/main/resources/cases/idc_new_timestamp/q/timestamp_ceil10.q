#Testcase for invalid test result

select
t0, timestamp_ceil(t0),
t3, timestamp_ceil(t3),
s9, timestamp_ceil(s9)
from roundFunc where id=2