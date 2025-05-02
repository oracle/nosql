#
# idx_a_c_f has highest score (more equality preds)
#
select id1
from Foo t
where t.rec.a = 10 and t.rec.c.ca =any 3
