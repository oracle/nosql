#TestCase for valid timestamp

select
t0, quarter(t0),
s9, quarter(s9),
t.doc.t1, quarter(t.doc.t1)
from roundFunc t where id <2