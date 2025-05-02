#testcase for string and any pattern

select
parse_to_timestamp(t.doc.str1,t.doc.pattern1),
parse_to_timestamp(t.doc.str2,t.doc.pattern2),
parse_to_timestamp(t.doc.str3,t.doc.pattern3),
parse_to_timestamp(t.doc.str4,"yyyy-MM-dd'T'HH:mm:ss")
from roundFunc t where id=6