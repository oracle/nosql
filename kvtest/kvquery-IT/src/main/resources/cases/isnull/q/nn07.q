select id
from Foo f
where f.address.state is not null and f.age is not null
