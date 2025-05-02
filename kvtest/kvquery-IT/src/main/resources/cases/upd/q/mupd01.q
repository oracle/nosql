update users
set name = upper(name),
set age = age + 1
where sid = 0

select id, name, age
from users
where sid = 0
