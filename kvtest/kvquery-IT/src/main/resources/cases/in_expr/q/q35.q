select id
from foo f
where exists f.info.phones[$element.num in (3, 5)] and
      exists f.info.phones[($element.num, $element.kind) in ((4, "h"), (6, "c"), (6, "s"))]
