#TestCase for valid timestamp

select
t0, timestamp_ceil(t0),
s9, timestamp_ceil(s9),
t.doc.t1, timestamp_ceil(t.doc.t1)
from roundFunc t where id <2

