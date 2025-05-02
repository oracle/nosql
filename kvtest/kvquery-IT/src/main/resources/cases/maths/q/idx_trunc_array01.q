#trunc with functional index

select m.id,m.doc.doubArr
from math_test m
where exists m.doc.doubArr[trunc($element) = trunc(123.456)]
order by id

