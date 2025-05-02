select /* FORCE_PRIMARY_INDEX(Foo) */id
from bar as $t, unnest($t.info.addresses[].phones[][][] as $phone)
where $phone.areacode = 400
