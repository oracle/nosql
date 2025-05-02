select /*+ FORCE_INDEX(nestedTable idx_age_areacode_kind) */ id
from nestedTable nt
where nt.addresses.phones[][$element.number in (31, 50, 70)].kind =any "home"
