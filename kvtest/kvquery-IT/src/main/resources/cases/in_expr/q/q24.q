select id
from foo f
where f.info.bar1 = 7 and
      f.info.bar1 in (7, 4, 3) and
      f.info.bar2 in (3, 3.4, 3.6) and
      f.info.bar2 >= 3.4

