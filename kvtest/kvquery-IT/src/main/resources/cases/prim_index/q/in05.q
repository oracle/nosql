select id1, id2, id3, id4
from foo
where (id3, id1, id4) in (("tok0", 3, "id4-3"), ("tok1", 4, "id4-4"))
