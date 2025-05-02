select id, 
       case when f.address.city = "Boston" then f.age = 11
            when f.address.city = "Salem"  then f.lastname
            when f.address.city = "San Fransisco" then f.age
       end
from Foo f
