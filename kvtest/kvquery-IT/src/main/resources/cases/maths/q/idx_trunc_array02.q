#trunc with functional index

select m.id,m.doc.doubArr
from math_test m
where  trunc(m.doc.doubArr[]) =any trunc(123.456)
order by id

