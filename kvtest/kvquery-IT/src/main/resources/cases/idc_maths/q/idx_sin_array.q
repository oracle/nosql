#test to check on sin function index on array elements

select m.id, m.numArr
from functional_test m
where exists m.numArr[sin($element) = sin(1)]
order by id