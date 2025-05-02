#Testcase for valid string(standard pattern)

select
parse_to_timestamp(t.doc.str4),
parse_to_timestamp("2100-02-28T21:50:30"),
parse_to_timestamp("9999-12-31T23:59:59.999999999")
from roundFunc t where id=6
