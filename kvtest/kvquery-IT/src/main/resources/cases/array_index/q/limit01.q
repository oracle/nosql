select id
from foo f
where f.rec.b[] <any 0
limit 3
