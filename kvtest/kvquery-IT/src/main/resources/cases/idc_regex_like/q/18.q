#Test Description: expression includes array slice step for source
#Expected result: return true

select id,id1,age,avg 
from playerinfo p 
where regex_like(cast(p.stats1[1] as string),"2.*")