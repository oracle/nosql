#test to check on radians function index on array elements

select m.id, m.numArr
from functional_test m
where exists m.numArr[radians($element) >0]
order by id