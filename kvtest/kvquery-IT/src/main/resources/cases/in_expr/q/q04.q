select id
from foo f
where f.info.bar1 in (4, 3, seq_concat(), null) and
      f.info.bar2 in (3.5, 3.6, null, seq_concat(), 3.1, 3.2) and
      "c" <= f.info.bar3 and f.info.bar3 < "p" 
