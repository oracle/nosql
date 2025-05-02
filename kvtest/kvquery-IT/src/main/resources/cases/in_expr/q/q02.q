select id
from foo f
where f.info.bar1 in (6, 3, seq_concat(), null) and
      f.info.bar2 in (3.5, 3.6, null, seq_concat(), 3.4, 3.0)

