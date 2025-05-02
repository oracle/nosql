update foo f
set f.record = null
where id = 0


select f.record
from foo f
where id = 0
