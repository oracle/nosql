
select *
from nested tables (
     users u
     descendants(users.folders f, users.folders.messages m, users.photos p))
where u.uid = 10
