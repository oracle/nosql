select id
from foo f
where f.info.bar1 in (7, 2, seq_concat(), null) and
      (foo1, f.info.bar2) in ((7, 3.5), (4, 3.9)) and
      "a" <= f.info.bar3 and f.info.bar3 < "p" 
