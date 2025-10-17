update users u
set u.info.height = $ + 5
where sid1 = 3 and (sid2 = 4 or sid2 = 5)

select pid1, pid2, u.info.height
from users u
where sid1 = 3 and (sid2 = 4 or sid2 = 5)