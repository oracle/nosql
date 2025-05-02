select st.id,
    length([][]) as l0,
    length(['abc'][]) as l1,
    length(['xy', 'z'][]) as l2,
    length(st.jsn)
from stringsTable2 st ORDER BY id
