#
# TODO: detect always true
#
select id
from Foo f
where f.address.phones[0].work is not null and f.address.phones[0].work = 605
