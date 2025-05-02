#test to check on log10 function index on json array elements

select m.id, m.doc.douArr
from functional_test m
where exists m.doc.douArr[log10($element) >0]
order by id