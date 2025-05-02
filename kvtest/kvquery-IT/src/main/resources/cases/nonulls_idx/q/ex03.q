select id
from foo f
where exists f.info.address.phones[].kind

