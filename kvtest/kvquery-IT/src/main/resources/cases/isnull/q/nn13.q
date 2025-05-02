select id
from Foo f
where f.address.phones[1].work <= 518 and f.address.phones[0].work is not null
