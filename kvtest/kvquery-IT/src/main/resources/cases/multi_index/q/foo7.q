#
# a_c_f, a_rf, and a_id2 have same score
# a_rf and a_id2 have same number of fields
# a_id2 wins because it is declared first
#
select *
from Foo t
where id3 > 0 and  0 < t.rec.a and t.rec.a <= 10
