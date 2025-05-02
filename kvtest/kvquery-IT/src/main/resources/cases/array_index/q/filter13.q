select id
from foo f
where exists f.rec.b[]
