#Expression returns non json array with is null in projection

select id, t.array[] is null 
from sn t 
where t.array[5]