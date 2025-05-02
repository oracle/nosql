declare $uuid1_1 string; // "18acbcb9-137b-4fc8-99f7-812f20240356"
select uid2
from foo
where uid1 = $uuid1_1 and int = 1 and str = "Tom"
