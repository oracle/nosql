#test to check on acos function index on json array elements

select m.id, m.doc.douArr
from functional_test m
where exists m.doc.douArr[acos($element) >0]
order by id