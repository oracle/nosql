select id
from Foo f
where f.address.state = "CA" and f.address.state is not null
