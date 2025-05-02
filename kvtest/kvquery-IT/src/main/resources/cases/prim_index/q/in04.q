select id1, id2, id3, id4
from foo
where (id1, id4, age) in ((3, "id4-3", 13), (4, "id4-4", 14))
