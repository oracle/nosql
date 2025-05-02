#Edge test cases

select
t0,
timestamp_trunc(t0,'year') as trunc_t0_year,
timestamp_trunc(t0,'iyear') as trunc_t0_iyear,
t3,
timestamp_trunc(t3,'year') as trunc_t3_year,
timestamp_trunc(t3,'iyear') as trunc_t3_iyear,

s9,
timestamp_trunc(s9,'quarter') as trunc_s9_quarter,
timestamp_trunc(s9,'month') as trunc_s9_month,
s3,
timestamp_trunc(s3,'quarter') as trunc_s6_quarter,
timestamp_trunc(s3,'month') as trunc_s6_month,

t.doc.t1 as t1,
timestamp_trunc(t.doc.t1,'week') as trunc_t1_week,
timestamp_trunc(t.doc.t1,'iweek') as trunc_t1_iweek,
timestamp_trunc(t.doc.t1,'day') as trunc_t1_day,
t.doc.t2 as t2,
timestamp_trunc(t.doc.t2,'week') as trunc_t2_week,
timestamp_trunc(t.doc.t2,'iweek') as trunc_t2_iweek,
timestamp_trunc(t.doc.t2,'day') as trunc_t2_day,

t.doc.t3 as t3,
timestamp_trunc(t.doc.t3,'hour') as trunc_t3_hour,
t.doc.t4 as t4,
timestamp_trunc(t.doc.t4,'hour') as trunc_t4_hour,

t.doc.t5 as t5,
timestamp_trunc(t.doc.t5,'minute') as trunc_t5_minute,
t.doc.t6 as t6,
timestamp_trunc(t.doc.t6,'minute') as trunc_t6_minute,


t.doc.t7 as t7,
timestamp_trunc(t.doc.t7,'second') as trunc_t7_second,
t.doc.t8 as t8,
timestamp_trunc(t.doc.t8,'second') as trunc_t8_second
from roundFunc t
where id =3

