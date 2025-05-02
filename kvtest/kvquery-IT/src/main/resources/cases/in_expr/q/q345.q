select *
from foo
where id in seq_concat(1, 2, 3)
