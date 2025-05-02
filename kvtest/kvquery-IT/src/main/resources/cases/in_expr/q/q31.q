select id
from foo f
where exists f.info.phones[($element.num, $element.kind) in ((3, "a"), (5, "b"))]
