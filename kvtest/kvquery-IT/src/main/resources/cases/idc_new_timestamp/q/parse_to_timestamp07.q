#TestCase for jsonCollection table

select
parse_to_timestamp(t.str3,t.pattern3),
parse_to_timestamp(t.str4,"yyyy-MM-dd'T'HH:mm:ss")
from  jsonCollection_test t where id=1