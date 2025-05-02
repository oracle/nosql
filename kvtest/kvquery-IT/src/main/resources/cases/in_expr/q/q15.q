#
# json int field, vs EMPTY key
#
select id
from foo f
where f.info.bar1 in (6, seq_concat())
