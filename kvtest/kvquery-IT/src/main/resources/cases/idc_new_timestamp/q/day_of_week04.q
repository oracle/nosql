#Edge test cases

select
t0,
day_of_week(t0),
t3,
day_of_week(t3),
s9,
day_of_week(s9),
s3,
day_of_week(s3),
t.doc.t1 as t1,
day_of_week(t.doc.t1),
t.doc.t3 as t3,
day_of_week(t.doc.t3),
t.doc.t9 as t9,
day_of_week(t.doc.t9),
t.doc.t10 as t10,
day_of_week(t.doc.t10),
t.doc.t11 as t11,
day_of_week(t.doc.t11),
t.doc.t12 as t12,
day_of_week(t.doc.t12),
t.doc.t13 as t13,
day_of_week(t.doc.t13),
t.doc.t14 as t14,
day_of_week(t.doc.t14),
t.doc.t15 as t15,
day_of_week(t.doc.t15)
from roundFunc t
where id =3

