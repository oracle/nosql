select id,
    replace([][], 'a', 'b') as r10,
    replace(['abc'][], 'bc', 'Z') as r11,
    replace(['abc', 'def'][], 'a', 'A') as r12,
    replace('abc', [][], 'b') as r20,
    replace('abc', ['bc'][], 'Z') as r21,
    replace('abc', ['abc', 'def'][], 'A') as r22,
    replace('abc', 'b', [][]) as r30,
    replace('abc', 'b', ['Z'][]) as r31,
    replace('abc', 'b', ['XYZ', 'W'][]) as r32
from
 stringsTable2 ORDER BY id