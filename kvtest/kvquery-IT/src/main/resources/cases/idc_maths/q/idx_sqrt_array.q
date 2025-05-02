#test to check on sqrt function index on array elements

select m.id, m.numArr
from functional_test m
where exists m.numArr[sqrt($element) >0]
order by id