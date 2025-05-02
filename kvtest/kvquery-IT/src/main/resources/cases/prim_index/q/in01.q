select *
from foo
where (id1, id2) in ((3, 30.0), (4, 42.0))
#where id1 = 3 and id2 = 30.0 or id1 = 4 and id2 = 42
