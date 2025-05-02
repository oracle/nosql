select id
from foo f
where f.info.addresses.phones.values().values().areacode =any 408
