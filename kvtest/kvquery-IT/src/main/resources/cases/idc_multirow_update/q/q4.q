update users t
add t.info.friends 'Jerry',
put t.info {"hobbies":["Cooking", "Music"]},
remove t.info.address.street
where sid1 = 3 and sid2 = 4 and pid1 = 3 and pid2 < 3

select pid1, pid2, info
from users
where sid1 = 3 and sid2 = 4
