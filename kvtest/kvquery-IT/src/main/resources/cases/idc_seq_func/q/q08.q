#Test: seq_concat() with json null values as argument.

select id1, 
       seq_concat(p.json.Shikhar.stats1.Tests.bf,p.json.Shikhar.stats3.century.odi) as jsonnullTests
from playerinfo p
where id1=3
