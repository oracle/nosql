#TestCase for valid timestamp

select
t0, timestamp_round(t0),
s9, timestamp_round(s9),
t.doc.t1, timestamp_round(t.doc.t1)
from roundFunc t where id <2