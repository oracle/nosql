#Testcase for invalid timestamp, pattern or timezone

select
format_timestamp("2021-11-2621:50:30.999999","yyyy-MD")
from roundFunc where id=1