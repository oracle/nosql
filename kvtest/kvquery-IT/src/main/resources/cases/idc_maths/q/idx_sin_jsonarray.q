#test to check on sin function index on json array elements

select m.id, m.doc.douArr
from functional_test m
where exists m.doc.douArr[sin($element) >0]
order by id