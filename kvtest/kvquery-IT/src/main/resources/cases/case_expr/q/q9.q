select case when f.address.city = "Boston" then f.id else f.age end
from Foo f
