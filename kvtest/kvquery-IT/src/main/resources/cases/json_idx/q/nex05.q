select id
from foo f
where not exists f.info.address.phones[].kind
