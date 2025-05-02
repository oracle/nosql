update Foo f
remove f.info.children.values()
where id = 13

select f.info.children
from Foo f
where id = 13
