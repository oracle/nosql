#test to check on abs function index on array elements

select m.id, m.numArr
from math_test m
where exists m.numArr[abs($element) = 30]
order by id

