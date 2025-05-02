select id
from foo f
where f.info.bar1 = 7 and
      f.info.bar2 in (3.0, 3.1, 3.5, 3.9) and
      (f.info.bar2, f.info.bar3) in ((3.1, "g"), (3.9, "d"))
