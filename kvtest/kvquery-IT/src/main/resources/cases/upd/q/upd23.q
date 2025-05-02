update foo f
add f.info.children.George.friends 1 seq_concat("Dave", "Mark", "Marry")
where id = 20

select f.info.children.George.friends
from foo f
where id = 20
