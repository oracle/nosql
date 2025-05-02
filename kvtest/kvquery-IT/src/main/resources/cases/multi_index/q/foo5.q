#
# a_c_f, a_rf, and a_id2 have same score
# a_id2 wins because it has less fields than a_c_f and is processed before a_rf
#
select id1, id2, id3, t.rec.c[0]
from Foo t
where id1 = 0 and 0 < t.rec.a and t.rec.a <= 10
