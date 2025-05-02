select $v, count(*)
from profile.messages $m, profile.inbox $in, unnest($m.content.views[] as $v)
where $m.uid = $in.uid and $m.msgid = $in.msgid
group by $v
