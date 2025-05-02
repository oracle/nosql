#Test: seq_count() with sql null values as argument.

select id1, 
       seq_count(p.age) as sqlnullTests
from playerinfo p
where id1=5
