select id
from foo f
where exists f.info.phones[$element.kind in ("a", "b")]
