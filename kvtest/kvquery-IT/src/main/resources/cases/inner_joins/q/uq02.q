select $v, count(*)
from profile.messages m, unnest(m.content.views[] as $v)
group by $v
