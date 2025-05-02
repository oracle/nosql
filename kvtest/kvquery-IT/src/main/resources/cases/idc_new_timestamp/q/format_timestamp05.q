#Edge test cases

select
t0,
format_timestamp(t0,t.doc.c1,'America/New_York'),
format_timestamp(t3,t.doc.c1,'Asia/Kolkata')
from roundFunc t where id =5