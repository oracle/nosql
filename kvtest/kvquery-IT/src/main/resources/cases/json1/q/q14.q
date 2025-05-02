#
# arithm op with json data
#
select { "id" : id, "age" : $age + 10 }
from foo f,
     case when f.info.children.John.age > 0 then f.info.children.John.age
                                            else f.info.children.Mark.age
     end as $age

