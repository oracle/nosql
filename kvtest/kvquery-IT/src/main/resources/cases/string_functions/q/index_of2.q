select st.id,
    index_of(st.jsn, 'a') as a1,
    index_of(st.jsn, 'c', 10) as c1,
    index_of([][], 'a') as i10,
    index_of(['abc'][], 'bc') as i11,
    index_of(['abc','xyz'][], 'y') as i12,
    index_of(' abc', [][]) as i20,
    index_of(' abc', ['abc'][]) as i21,
    index_of(' abc', ['abc','xyz'][]) as i22,
    index_of(' abc', 'c', [][]) as i30,
    index_of(' abc', 'c', [1][]) as i31,
    index_of(' abc', 'c', [1,2][]) as i32
from
    stringsTable2 st ORDER BY id