#Testcase for invalid test result

select
t0, timestamp_round(t0, "iyear"),
t3, timestamp_round(t3),
s9, timestamp_round(s9)
from roundFunc where id=2