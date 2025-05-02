select st.id, st.jsn,
  substring( st.jsn.a1, 1),
  substring( st.jsn.b1, 0),
  substring( st.jsn.a1[], 1),
  substring( st.jsn.c1[], 0),
  [substring( st.jsn.e1[], 0)]
from stringsTable2 st ORDER BY st.id