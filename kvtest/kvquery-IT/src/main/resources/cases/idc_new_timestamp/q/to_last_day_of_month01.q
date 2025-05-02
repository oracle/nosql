#TestCase for timestamp is null

select
to_last_day_of_month(t0) as to_last_day_of_month_null
from roundFunc where id=1