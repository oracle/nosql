select id1, id2, id3, id4
from foo
where (id1, id3, id4) in ((3, "tok0", "id4-3"), (4, "tok1", "id4-4"))

#where (id1 = 3 and id3 = "tok0" and id4 = "id4-3") or
#      (id1 = 4 and id3 = "tok1" and id4 = "id4-4")
