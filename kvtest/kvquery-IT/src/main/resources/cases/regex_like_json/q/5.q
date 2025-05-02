# regex like operator source parameter is an integer
#
select id, regex_like(f.info.age, "Ava.*")
from Foo f
where id = 4


