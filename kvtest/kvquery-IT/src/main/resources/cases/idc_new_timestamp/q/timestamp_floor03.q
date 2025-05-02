#TestCase for valid timestamp

select
t0, timestamp_floor(t0),
s9, timestamp_floor(s9),
t.doc.t1, timestamp_floor(t.doc.t1)
from roundFunc t where id <2