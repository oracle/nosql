update foo f
set f.rec.c.keys()[$element = "c1"] = "c5"
where id = 2
returning f.rec.c
