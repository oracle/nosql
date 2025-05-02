#TestCase for valid timestamp

select
t0, to_last_day_of_month(t0),
s9, to_last_day_of_month(s9),
t.doc.t1, to_last_day_of_month(t.doc.t1)
from roundFunc t where id <2