select id
from foo f
where f.info.bar1 = 7 and
      (f.info.bar1, f.info.bar2) in ((7, 3), (4, 3.4), (3, 3.6)) and
      3.1 <= f.info.bar2 and f.info.bar2 < 3.8
