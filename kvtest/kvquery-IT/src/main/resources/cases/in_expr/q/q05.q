select id
from foo f
where (f.info.bar1, f.info.bar3) in ((8, "a"), (4, "a"), (6, ""),
                                     (seq_concat(), "d"), (null, null)) and
      f.info.bar2 in (3.9, null, seq_concat(), 3.1, 3.2) and
      101 <= f.info.bar4 and f.info.bar4 < 108 
