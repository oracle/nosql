update users u
set u.info.phones[].areacode = $ + 1,
add u.info.friends seq_concat("Ada", "Aris"),
add u.info.phones 2 { "areacode":876, "number":3872730, "kind":"home2" },
put u.info.address { "country": "USA" },
remove u.info.phones [$element.kind = "office"]
where sid1 = 0 and sid2 = 1

select pid1, pid2, $u.info, remaining_days($u) as days
from users $u
where sid1 = 0 and sid2 = 1