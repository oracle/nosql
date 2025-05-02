select seq_transform(t.doc.map.keys(), 
                     timestamp_round($sq1, t.doc.map.$sq1)) as dts 
from roundtest t 
where id = 0
