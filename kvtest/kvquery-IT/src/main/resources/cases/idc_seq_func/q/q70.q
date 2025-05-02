#Test seq_min: Expression having couple of seq_min() functions on map.
select id1, 
       seq_min(p.json.stats.values()) as min1, seq_min(p.json.stats1.Odi.values()) as min2
from playerinfo p
