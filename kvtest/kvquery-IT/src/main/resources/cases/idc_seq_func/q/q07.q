#Test: seq_concat() with sql null values as argument.

select id1, 
       seq_concat(age,avg,p.stats1.T20.inns) as sqlnullTests
from playerinfo p
where id1=3
