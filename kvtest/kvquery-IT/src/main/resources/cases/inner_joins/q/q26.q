select *
from A a, A.B b
where a.s = "a1" and a.sid = b.sid
order by a.id
