select id
from foo f
where (f.info.bar1, f.info.bar2) in ((8, 3.4), (2, 3.9), (9, 3.0),
                                     (8.0, 3.4), (9.0, 3)) and
      f.info.bar2 in (3.4, 3.5, 3.5, 3.5, 3.40, 3.40)
