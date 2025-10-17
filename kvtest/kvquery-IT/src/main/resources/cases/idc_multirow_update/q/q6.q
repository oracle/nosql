update users
set age = age + 1
where sid1 = 1 and sid2 = 2 and pid1 = 1

select pid1, pid2, age
from users
where sid1 = 1 and sid2 = 2
