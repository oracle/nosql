# st.jsn.b1 , st.jsn.b1[] , st.jsn.c1 , st.jsn.c1[0]
# st.arrStr[0] , st.arrInt[0]
# st.mapStr.a[0] , st.mapInt.a[0]
# st.recStr.fmap.a[0] , st.recFlt.fmap.a[0]
# st.arrrecStr[0].fmap.a[0] , st.arrrecFlt[0].fmap.a[0]

select id, upper(st.str),
           upper(st.jsn)
from stringsTable2 st ORDER BY id