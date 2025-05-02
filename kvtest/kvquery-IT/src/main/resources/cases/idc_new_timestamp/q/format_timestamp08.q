#TestCase for jsonCollection table

select
t.l3,
format_timestamp(t.l3,"yyyy-MM-dd",'GMT+05:00'),
format_timestamp(t.l3,"yyyy-MM-dd'T'HH:mm:ssXXXXX",'Asia/Kolkata'),
format_timestamp(t.s6,"EEE, dd MMM yyyy HH:mm:ss zzzz",'America/New_York'),
format_timestamp(t.s6,"yyyy-DDD",'UTC')
from  jsonCollection_test t where id=1