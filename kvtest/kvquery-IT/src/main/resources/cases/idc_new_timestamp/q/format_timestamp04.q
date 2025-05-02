#A valid timestamp and pattern with timezone

select
t0,
format_timestamp(t0,t.doc.f1,'GMT+05:00'),
format_timestamp(t0,"EEE, dd MMM yyyy HH:mm:ss zzzz",'Asia/Kolkata'),
format_timestamp(t0,t.doc.f3,'America/New_York'),
format_timestamp(t0,t.doc.f4,'UTC'),
format_timestamp(t0,t.doc.f5,'GMT+00:00'),
format_timestamp(t0,t.doc.f6,'Asia/Kolkata'),
format_timestamp(t0,t.doc.f7,'America/Los_Angeles'),
format_timestamp(t0,t.doc.f8,'America/Chicago'),
format_timestamp(t0,t.doc.f9,'Asia/Kolkata'),
format_timestamp(t0,t.doc.f10,'America/Los_Angeles'),
format_timestamp(t0,t.doc.f11,'Asia/Kolkata'),
format_timestamp(t0,t.doc.f12,'Asia/Dhaka'),
format_timestamp(t0,t.doc.f13,'Australia/Sydney'),
format_timestamp(t0,t.doc.f14,'GMT'),
format_timestamp(t0,t.doc.f15,'Asia/Kolkata'),
format_timestamp(t0, 'MMMdd yyyyzzzz','America/Chicago'),
format_timestamp(t0, 'HH-mm-ss.SSSSSS','Australia/Sydney'),
format_timestamp(t0, 'MM/dd/yyyy HH:mm zzzz', 'America/New_York'),
format_timestamp(t0, 'MMM dd yyyy zzzz', 'GMT+05:00'),
format_timestamp(t0, "MM/dd/yy'T'HH:mm:SS.SSS zzzz", 'UTC'),
format_timestamp(t0, 'MMM,dd HH-mm-SS','Asia/Kolkata')
from roundFunc t where id =4


