select st.id,
  substring( 'abc', [][]),
  substring( 'abc', [1][]),
  substring( 'abc', [1,2][]),
  substring( 'abc', 0, [][]),
  substring( 'abc', 0, [1][]),
  substring( 'abc', 0, [1,2][])
from stringsTable2 st ORDER BY st.id