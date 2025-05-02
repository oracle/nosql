select p.uid, $r as receiver, count(*) as cnt
from profile p, profile.messages $m, unnest($m.content.receivers[] $r)
where p.uid = $m.uid and p.userName = $r
group by p.uid, $r
