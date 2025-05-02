select id
from foo f
where f.info.bar1 in (null, 6, 6, 3, null, seq_concat(), 3, 6, 6, seq_concat())
