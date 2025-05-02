select st.id,
    index_of(st.jsn, 'a', '1') as a1
from
 stringsTable2 st ORDER BY id