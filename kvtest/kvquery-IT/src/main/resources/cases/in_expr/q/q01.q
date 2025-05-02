select id
from foo f
where f.info.bar1 in (6, 3, seq_concat(), null)

