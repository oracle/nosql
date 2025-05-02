#
# typed int field, vs string key
#
declare $k8 string; // "p"
select id
from foo f
where foo1 in (6, $k8)
