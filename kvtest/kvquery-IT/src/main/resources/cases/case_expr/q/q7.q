select id, 
       [ 
         case
         when $f.address.state = "MA" then $f.address.phones[].work
         else $f.address.phones[].home
         end
       ]
from Foo $f
