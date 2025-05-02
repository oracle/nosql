select id, seq_min(t.info.firstName)
from foo t
where id = 20
