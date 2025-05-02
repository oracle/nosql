declare $foo integer
select lastName,
       $C.children.values(($key = "Lisa" or $key = "Mark" or $key = "Matt") and
                          $pos < 10)
from Complex $C
