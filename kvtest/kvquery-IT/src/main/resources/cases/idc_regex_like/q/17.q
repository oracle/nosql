#Test Description: expression includes array filter step for source
#Expected result: return true

select id,id1,age,avg 
from playerinfo p 
where regex_like(cast(p.stats1[$element=11] as string),"1.*")