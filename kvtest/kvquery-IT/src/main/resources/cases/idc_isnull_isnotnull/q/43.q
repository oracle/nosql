#Expression returns non json array with is not null in predicate 

select id,array 
from sn t 
where t.array[].AXIS is not null