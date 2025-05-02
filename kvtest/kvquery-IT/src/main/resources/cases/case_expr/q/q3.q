select id, case when f.address.city = "Boston" then f.age = 11 else f.lastname end
from Foo f
where f.address.state = "MA" and lastName >= "last1"
