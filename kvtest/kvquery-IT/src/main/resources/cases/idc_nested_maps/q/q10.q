select /*+ FORCE_INDEX(nestedTable idx_keys_number_kind) */ id
from nestedTable $nt
where exists $nt.addresses.values($value.city = "Portland").phones.values($value.values().areacode =any 118 and $value.values().number =any 61)
