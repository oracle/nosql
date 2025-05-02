#test to check on sqrt function index on json array elements

select m.id, m.doc.douArr
from functional_test m
where exists m.doc.douArr[sqrt($element) >0]
order by id