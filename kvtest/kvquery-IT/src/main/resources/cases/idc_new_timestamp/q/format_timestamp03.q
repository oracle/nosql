#A valid timestamp and pattern without timezone

select
t0,
format_timestamp(t0,t.doc.f1),
format_timestamp(t0,t.doc.f2),
format_timestamp(t0,t.doc.f3),
format_timestamp(t0,t.doc.f4),
format_timestamp(t0,t.doc.f5),
format_timestamp(t0,t.doc.f6),
format_timestamp(t0,t.doc.f7),
format_timestamp(t0,t.doc.f8),
format_timestamp(t0,t.doc.f9),
format_timestamp(t0,t.doc.f10),
format_timestamp(t0,t.doc.f11),
format_timestamp(t0,t.doc.f12),
format_timestamp(t0,t.doc.f13),
format_timestamp(t0,t.doc.f14),
format_timestamp(t0,t.doc.f15) 
from roundFunc t where id =4

