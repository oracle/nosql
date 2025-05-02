update users 
set seqNo = null
where sid = 0 

select sid, id, seqNo
from users
where sid = 0
