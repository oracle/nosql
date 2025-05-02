#test to check on acos function index on array elements

select m.id, m.numArr
from functional_test m
where exists m.numArr[acos($element) >0]
order by id