update Foo f
set f.info.address.city = "Boston"
where id = 16

select *
from foo f
where id = 16
