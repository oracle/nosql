#Expression returns non json array with is not null in predicate and .values in projection

select id, t.array[].values 
from sn t
where t.array[].AXIS[].values is not null