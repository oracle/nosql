#Test: seq_concat() with atomic argument.

select id1, 
       seq_concat(name,age,ballsbowled,ballsplayed,strikerate,tier1rated,avg,fbin,bin,century,country,type,time) as AtomicStats
from playerinfo p
order by id
