select id, str1, str2, str3,
    replace(str1, str2, str3) as s123,
    replace(str1, str3, str2) as s132,
    replace(str1, str2, 'W') as s12W,
    replace(str1, str3, 'X') as s13X,
    replace(str2, str1) as s21,
    replace(str3, str1) as s31,
    replace(str1, NULL) as s1Null,
    replace("aBa", "a", str1) as sa1,
    replace("aBa", "a", [][]) as saempty,
    replace("aBa", "a", [1, 2][]) as samulti
from
 stringsTable ORDER BY id
