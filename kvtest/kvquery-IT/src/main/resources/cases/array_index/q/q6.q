#
# partial key and range; only one multi-key pred pushed. Notice that
# compared to q5, we have only changed the order of 2 predicates, but
# we get different result w.r.t. duplicates.
#
select id
from Foo t
where t.rec.a = 10 and t.rec.c.ca <any 10 and 10 <=any t.rec.c.ca
