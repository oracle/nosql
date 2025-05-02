#test to check on ceil function index on array elements

select m.id, m.douArr
from functional_test m
where exists m.douArr[ceil($element) = 2]
order by id