select id1, id2, id3, id4, age
from Foo
where age = 15 or age = 16 or age = 17
order by age, id1, id2, id3
limit 32 offset 25
