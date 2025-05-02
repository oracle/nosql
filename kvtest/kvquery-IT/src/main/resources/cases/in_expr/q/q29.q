select id
from foo f
where f.info.bar1 in (7, 2, seq_concat(), null) and
      (foo1, f.info.bar2, ltrim(f.info.bar3)) in ((4, 3.9, "d"), (3, 4, "g")) and
      100 <= f.info.bar4 and f.info.bar4 < 108
