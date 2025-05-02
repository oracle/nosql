select id, 
       case when age > 12 then children
            when $f.address.city = "Boston" then age = 11
            when $f.address.city = "Salem"  then lastname
            when $f.address.city = "San Fransisco" then age
            else $f.address.($f.address.ptr)
       end
from Foo $f
