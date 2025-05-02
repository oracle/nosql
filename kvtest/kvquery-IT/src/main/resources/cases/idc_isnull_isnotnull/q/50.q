#Expression returns non json array with is not null in predicate and projection

select id, array is not null 
from sn t
where t.array[].AXIS is not null