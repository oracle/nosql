select id, str1, replace(str1, 'a') as a1,
    replace(str1, 'c') as c1,
    replace(str1, 'a', 'AAA') as a2,
    replace(str1, 'abc', 'ABC') as abc1,
    replace(str1, '', 'abc') as empty,
    replace(str1, ' ', '#') as space,
    replace(str1, '😋', '😸') as smiley
from
 stringsTable ORDER BY id
