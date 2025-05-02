#test to check on radians function index on map values

select m.id, m.douMap
from functional_test m
where exists m.douMap.values(radians($value) !=0)
order by id
