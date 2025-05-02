#Edge test cases

select
t0,
timestamp_round(t0,'year') as round_t0_year,
timestamp_round(t0,'iyear') as round_t0_iyear,
t3,
timestamp_round(t3,'year') as round_t3_year,
timestamp_round(t3,'iyear') as round_t3_iyear,

s9,
timestamp_round(s9,'quarter') as round_s9_quarter,
timestamp_round(s9,'month') as round_s9_month,
s3,
timestamp_round(s3,'quarter') as round_s6_quarter,
timestamp_round(s3,'month') as round_s6_month,

t.doc.t1 as t1,
timestamp_round(t.doc.t1,'week') as round_t1_week,
timestamp_round(t.doc.t1,'iweek') as round_t1_iweek,
timestamp_round(t.doc.t1,'day') as round_t1_day,
t.doc.t2 as t2,
timestamp_round(t.doc.t2,'week') as round_t2_week,
timestamp_round(t.doc.t2,'iweek') as round_t2_iweek,
timestamp_round(t.doc.t2,'day') as round_t2_day,

t.doc.t3 as t3,
timestamp_round(t.doc.t3,'hour') as round_t3_hour,
t.doc.t4 as t4,
timestamp_round(t.doc.t4,'hour') as round_t4_hour,

t.doc.t5 as t5,
timestamp_round(t.doc.t5,'minute') as round_t5_minute,
t.doc.t6 as t6,
timestamp_round(t.doc.t6,'minute') as round_t6_minute,


t.doc.t7 as t7,
timestamp_round(t.doc.t7,'second') as round_t7_second,
t.doc.t8 as t8,
timestamp_round(t.doc.t8,'second') as round_t8_second
from roundFunc t
where id =3

