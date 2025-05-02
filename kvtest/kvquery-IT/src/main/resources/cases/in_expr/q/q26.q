select id
from foo f
where f.info.bar1 = 7 and
      (f.info.bar1, f.info.bar2) in ((7, 3), (7, 3.5), (3, 3.6)) and
      3 <= f.info.bar2 and f.info.bar2 <= 3.5 and
      f.info.bar3 <= "t"
