update foo f
set f.info.phones[0] = f.info
where id = 27

select f.info.phones
from foo f
where id = 27
