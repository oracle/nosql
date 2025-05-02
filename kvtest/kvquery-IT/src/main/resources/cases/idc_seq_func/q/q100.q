#Test seq_max(): Expression using all the arith oprtrs on map.

select id1, 
       seq_max(p.json.stats1.T20.values() + p.json.Rohit.values() * p.json.Virat.values() - p.json.Shikhar.values() / p.json.stats1.ODi.values()) as arithOprtrs
from playerinfo p