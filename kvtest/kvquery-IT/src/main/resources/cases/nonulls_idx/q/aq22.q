select id, f.record.int
from foo f
where f.info[].children[].Anna[].friends =any null
