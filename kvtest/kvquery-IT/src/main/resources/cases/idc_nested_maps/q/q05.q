select /*+ FORCE_INDEX(nestedTable idx_age_areacode_kind) */ id
from nestedTable $nt
where exists $nt.addresses.values($value.city = "Santa Cruz").phones.values($value.values().areacode =any 408)
