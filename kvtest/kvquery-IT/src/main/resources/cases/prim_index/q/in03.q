select id1, id2, id3, id4
from foo
where (id1, age, id4) in ((3, 13, "id4-3"), (4, 14, "id4-4"))

#where (id1 = 3 and age = 13 and id4 = "id4-3") or
#      (id1 = 4 and age = 14 and id4 = "id4-4")
