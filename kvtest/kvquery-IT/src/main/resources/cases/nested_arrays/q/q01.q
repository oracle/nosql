select id
from foo f
where f.info.addresses.phones[].areacode =any 408
