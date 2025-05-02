select lastName,
       C.children.values($key = "John")
from Complex C
