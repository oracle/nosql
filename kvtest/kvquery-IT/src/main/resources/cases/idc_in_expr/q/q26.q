select id
from ComplexType f
where f.address.state = "CA" and
(f.id, f.flt, f.firstName) in ((0, 10.5, "first0"), (4, 3.4, "first2"), (3, 3.6, "first3")) and
10 <= f.age and f.lng <= 0
