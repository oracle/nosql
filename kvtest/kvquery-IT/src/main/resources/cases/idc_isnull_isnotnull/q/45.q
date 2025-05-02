#Expression returns non json array with is not null in predicate using values

select id, array 
from sn t
where t.array[].AXIS[].values is not null