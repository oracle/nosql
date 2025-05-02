#test to check on ceil function index on map values

select m.id, m.douMap
from functional_test m
where exists m.douMap.values(ceil($value) = ceil(1.5))
order by id
