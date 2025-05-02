update foo f
put f.info.children { "foo" : $ }
where id = 26

select f.info.children
from foo f
where id = 26
