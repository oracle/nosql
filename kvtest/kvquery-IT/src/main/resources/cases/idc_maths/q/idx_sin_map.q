#test to check on sin function index on map values

select m.id, m.numMap
from functional_test m
where exists m.numMap.values(sin($value) !=0)
order by id
