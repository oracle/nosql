#
# idx_a_rf, idx_a_c_f, idx_d_f are all covering and all have equal score.
# They all beat idx_id3 because they are covering and their score == 32
# (which is the score of idx_id3, not taking into account its key completeness)
# a_rf is the winner because it's a simple index.
#
select id1, t.rec.f
from Foo t
where id3 = 0 and t.rec.f = 0
