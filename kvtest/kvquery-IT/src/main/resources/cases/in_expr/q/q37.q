select id
from foo f
where exists f.info.phones[$element.num = 5 and $element.kind in ("a", "b", "c")]
