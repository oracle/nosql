update users u
set u.info.height = $ + 5
where pid1 = 0 and u.info.height = 170

select pid1, pid2, u.info.height
from users u
where pid1 = 0 and u.info.height = 170