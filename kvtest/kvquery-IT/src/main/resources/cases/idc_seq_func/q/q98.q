#Test seq_max: Expression having couple of seq_max() functions on map.
select id1, 
       seq_max(p.json.stats.values()) as max1, seq_max(p.stats1.Odi.values()) as max2
from playerinfo p
