select id, 
       case
       when f.info.children.values().values($key = "friends") is of type (any)
       then f.info.children.values().values($key = "friends") = ["Anna", "John", "Maria"]
       else false
       end
from foo f
order by id
