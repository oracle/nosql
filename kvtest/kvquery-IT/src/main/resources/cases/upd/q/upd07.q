update Foo f
remove f.info.address.phones[].kind
where id = 6


select f.info.address.phones
from foo f
where id = 6
