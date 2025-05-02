select id
from Foo f
where f.address.state is null and f.address.city is null
