#TestCase for valid timestamp

select
t0, day_of_year(t0),
s9, day_of_year(s9),
t.doc.t1, day_of_year(t.doc.t1)
from roundFunc t where id <2