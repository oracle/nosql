select id1, str 
from foo f
where regex_like(f.address.city, "Be.keley")
