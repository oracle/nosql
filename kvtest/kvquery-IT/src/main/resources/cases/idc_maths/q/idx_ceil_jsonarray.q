#test to check on ceil function index on json array elements

select m.id, m.doc.numArr
from functional_test m
where exists m.doc.numArr[ceil($element) = ceil(0.8)]
order by id