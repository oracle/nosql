select id
from foo f
where (f.info.bar1) in (1, 2, (seq_concat(f.info.bar1)), null)
