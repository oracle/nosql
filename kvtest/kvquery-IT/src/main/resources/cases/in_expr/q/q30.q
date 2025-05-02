select id
from foo f
where exists f.info.phones[$element.num in (3, 5)]
