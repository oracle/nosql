select id
from foo f
where foo1 in (f.info.phones[].num)
