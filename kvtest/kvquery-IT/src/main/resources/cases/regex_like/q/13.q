select id1, str
from foo f
where regex_like(f.address.street, concat("Old ", "Tunnel", ".*"))
