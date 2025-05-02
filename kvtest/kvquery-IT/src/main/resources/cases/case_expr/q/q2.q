select id
from Foo f
where f.address.state = "MA" and
      (case when f.address.city = "Boston" then f.age = 11
            else f.lastName = "last3" end) and
      f.lastName >= "last1"

