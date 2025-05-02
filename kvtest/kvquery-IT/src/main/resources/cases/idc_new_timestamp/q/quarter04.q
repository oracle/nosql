#Edge test cases

select
t0,
quarter(t0),
t3,
quarter(t3),
s9,
quarter(s9),
s3,
quarter(s3),
t.doc.t1 as t1,
quarter(t.doc.t1),
t.doc.t3 as t3,
quarter(t.doc.t3),
t.doc.t9 as t9,
quarter(t.doc.t9),
t.doc.t10 as t10,
quarter(t.doc.t10),
t.doc.t11 as t11,
quarter(t.doc.t11),
t.doc.t12 as t12,
quarter(t.doc.t12),
t.doc.t13 as t13,
quarter(t.doc.t13),
t.doc.t14 as t14,
quarter(t.doc.t14),
t.doc.t15 as t15,
quarter(t.doc.t15)
from roundFunc t
where id =3

