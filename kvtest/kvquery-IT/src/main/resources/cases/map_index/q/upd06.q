#
# TODO: This update does not affect idx6 and idx7, but this is not detected
#
update foo f
set f.rec.c.values($key > "c3") = {"ca" : 1, "cb" : 10, "cc" : 100, "cd" : 200 }
where id = 2
