select id,
    '😋😋 😋_abc_😋😋',
    substring('😋😋 😋_abc_😋😋', 0, 3) as c03,
    substring('😋😋 😋_abc_😋😋', 0, 10) as c0_10,
    substring('😋😋 😋_abc_😋😋', 0, 11) as c0_11,
    substring('😋😋 😋_abc_😋😋', -1, 11) as c_1_11,
    substring('😋😋 😋_abc_😋😋', 1, 9) as c19,
    substring('😋😋 😋_abc_😋😋', 2, 7) as c27,
    substring('😋😋 😋_abc_😋😋', 3, 5) as c35,
    substring('😋😋 😋_abc_😋😋', 4, 3) as c43,
    substring('😋😋 😋_abc_😋😋', 5, 1) as c51,
    substring('😋😋 😋_abc_😋😋', 6, 0) as c60
from stringsTable2 ORDER BY id