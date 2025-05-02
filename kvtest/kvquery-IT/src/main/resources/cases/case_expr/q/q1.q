
select id
from Foo f
where f.address.state = "MA" and
      (case when f.address.city = "Boston" then f.age = 11 else f.lastname = "" end)
