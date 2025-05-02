update foo f
put f.rec.c[] { "c4" : {"ca" : 1, "cb" : 10, "cc" : 100, "cd" : 200 },
                "c5" : {"ca" : 1, "cb" : 20, "cc" : 200, "cd" : 300 }},
set f.rec.c.c5.cb = 40
where id = 2

