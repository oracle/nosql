#Edge test cases

select
t.doc.t1 as t1 , timestamp_ceil(t.doc.t1,"year"),
t.doc.t2 as t2 , timestamp_ceil(t.doc.t2,"month"),
t.doc.t3 as t3 , timestamp_ceil(t.doc.t3,"week"),
t.doc.t4 as t4 , timestamp_ceil(t.doc.t4,"day"),
t.doc.t5 as t5 , timestamp_ceil(t.doc.t5,"day"),
t.doc.t6 as t6 , timestamp_ceil(t.doc.t6,"day"),
t.doc.t7 as t7 , timestamp_ceil(t.doc.t7,"year")
from roundFunc t where id =2