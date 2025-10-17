update users
set seqNo = null
where sid1 = 0 and sid2 = 1

select pid1, pid2, seqNo
from users
where sid1 = 0 and sid2 = 1