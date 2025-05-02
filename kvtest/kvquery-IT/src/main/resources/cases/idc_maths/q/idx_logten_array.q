#test to check on log10 function index on array elements

select m.id, m.numArr
from functional_test m
where exists m.numArr[log10($element) >0]
order by id