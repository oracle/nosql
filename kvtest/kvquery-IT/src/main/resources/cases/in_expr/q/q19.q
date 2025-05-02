select id
from foo f
where (f.info.bar1, f.info.bar4) in ((8, 100), (4, 109), (6, 103)) and
      f.info.bar1 in (4, 5)
