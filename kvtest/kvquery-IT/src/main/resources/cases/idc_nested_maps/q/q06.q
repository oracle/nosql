select /*+ FORCE_PRIMARY_INDEX(nestedTable) */ id
from nestedTable $nt
where exists $nt.addresses.values($value.city = "Santa Cruz").phones.values($value.values().areacode =any 408)
