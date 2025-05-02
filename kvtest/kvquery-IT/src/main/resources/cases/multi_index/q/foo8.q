#
# idx_a_rf, idx_a_c_f, idx_d_f are all covering, but idx_id3 wins because
# it has complete key and none of the covering indexes have a score >= 32
# (which is the score of idx_id3, not taking into account its key completeness)
#
select id1, t.rec.f
from Foo t
where id3 = 0
