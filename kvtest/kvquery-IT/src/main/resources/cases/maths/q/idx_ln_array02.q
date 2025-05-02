#ln() with functional index

select m.id,m.doc.numArr
from math_test m
where ln(m.doc.numArr[]) =any ln(2)
order by id

