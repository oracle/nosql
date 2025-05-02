# st.arrStr[] returns all items in the array as result is null

select id,
  upper(st.arrStr[]) as up_seq,
  lower(st.arrStr[]) as lo_seq,
  upper( [][]),
  upper( ['aBc'][]),
  upper( ['x', 'Y'][]),
  lower( [][]),
  lower( ['AbC'][]),
  lower( ['X', 'y'][])
from stringsTable2 st ORDER BY id