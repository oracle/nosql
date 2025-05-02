select id
from nestedTable $nt
where exists $nt.addresses.values().phones.values().values($key = "phone7" and $value.number <any 50 and $value.kind =any "home")
