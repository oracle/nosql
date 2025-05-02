#
# TODO: detect always false
#
select id
from Foo f
where f.address.phones[0].work is null and f.address.phones[0].work = 605
