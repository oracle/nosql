#Test: seq_count() with json null values as argument.

select id1, 
       seq_count(p.json.Shikhar.stats3.century.odi) as jsonnullTests
from playerinfo p
where id1=3
