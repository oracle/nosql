#
# a_c_f wins with highest score (more equality preds)
#
select id1
from Foo t
where t.rec.a = 10 and t.rec.c.ca =any 3 and id2 > 0
