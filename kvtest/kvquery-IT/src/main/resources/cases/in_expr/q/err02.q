select id
from foo f
where (f.info.bar1, f.info.bar2) in ((8, 3.4), (2, 3.9, 5), (9, 3.0))
