#
# json int field, vs json null key
#
select id
from foo f
where f.info.bar1 in (6, null)
