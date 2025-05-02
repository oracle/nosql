select lastName,
       C.children.values(($key = "Lisa" or $key = "Mark" or $key = "Matt") and
                         $value.age > 10)
from Complex C
