#test to check on radians function index on json array elements

select m.id, m.doc.douArr
from functional_test m
where exists m.doc.douArr[radians($element) = radians(1.1)]
order by id