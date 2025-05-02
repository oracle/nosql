#Testcase for the string is null or pattern is null

select
parse_to_timestamp(t.doc.str5),
parse_to_timestamp(t.doc.str4,t.doc.str5)
from roundFunc t where id=6
