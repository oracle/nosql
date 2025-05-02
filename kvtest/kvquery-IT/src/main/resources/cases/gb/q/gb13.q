select min(ts3) as ts3min, 
       max(ts3) as ts3max, 
       min(seq_min(t.mts6.values())) as mts6min, 
       max(seq_max(t.mts6.values())) as mts6max 
from tsminmax t
