select /*+ FORCE_PRIMARY_INDEX(foo) */ 
     id
from foo f
where f.info.bar1 in (6, 3, seq_concat(), null)

