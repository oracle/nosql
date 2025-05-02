# st.jsn.b1 , st.jsn.b1[] , st.jsn.c1 , st.jsn.c1[0]
# st.arrStr[0] , st.arrInt[0]
# st.mapStr.a[0] , st.mapInt.a[0]
# st.recStr.fmap.a[0] , st.recFlt.fmap.a[0]
# st.arrrecStr[0].fmap.a[0] , st.arrrecFlt[0].fmap.a[0]

select id, upper(st.str),
           upper(st.jsn.a1), upper(st.jsn.b1), upper(st.jsn.c1),
           upper(st.arrStr[0]), upper(st.arrInt[0]),
           upper(st.mapStr.a[0]) , upper(st.mapInt.a[0]),
           upper(st.recStr.fmap.a[0]) , upper(st.recFlt.fmap.a[0]),
           upper(st.arrrecStr[0].fmap.a[0]) , upper(st.arrrecFlt[0].fmap.a[0])
from stringsTable2 st ORDER BY id