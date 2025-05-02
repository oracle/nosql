select id
from foo f
where (f.info.bar1, f.info.bar3, f.info.bar4) in ((8, "u", 100), (4, "v", 109), (6, "", 103))
