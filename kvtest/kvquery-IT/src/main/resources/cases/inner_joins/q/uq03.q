select $v, count(*)
from profile.messages $m, profile.inbox $in, $m.content.views[] as $v
where $m.uid = $in.uid and $m.msgid = $in.msgid
group by $v
