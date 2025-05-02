#
# Make sure that the index idx_g_c_f is used, but only the "g" pred is pushed
#
select /*+ FORCE_INDEX(Foo idx_g_c_f) */ id
from Foo t
where t.g = 5 and t.rec.c."values()".ca =any 10
