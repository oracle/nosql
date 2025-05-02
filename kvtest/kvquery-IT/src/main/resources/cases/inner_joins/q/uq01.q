select $s as size, count(*) as cnt
from profile.messages $m, profile.inbox $in, $m.content.size as $s
where $m.uid = $in.uid and $m.msgid = $in.msgid
group by $s
