#ln() with functional index

select m.id,m.doc.numArr
from math_test m
where exists m.doc.numArr[ln($element) = ln(2)]
order by id

