#TestCase for valid timestamp

select
t0, timestamp_trunc(t0),
s9, timestamp_trunc(s9),
t.doc.t1, timestamp_trunc(t.doc.t1)
from roundFunc t where id <2