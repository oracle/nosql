#TestCase for arrays

select
t.doc.arr,
parse_to_timestamp(t.doc.arr,"yyyy-MM-dd'T'HH:mm:ss.SSSSSS") as parse_to_timestamp_arr
from roundFunc t where id=0