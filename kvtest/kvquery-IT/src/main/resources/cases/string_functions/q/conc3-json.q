# select id, st.str ||
#            '- jsn.a1:' || st.jsn.a1 ||
#            '- jsn.b1:' || st.jsn.b1 ||
#            '- jsn.b1[]:' || st.jsn.b1[] ||
#            '- jsn.c1:' || st.jsn.c1 ||
#            '- jsn.c1[0]:' || st.jsn.c1[0] ||
#            '- arrStr[0]:' || st.arrStr[0] ||
#            '- arrInt[0]:' || st.arrInt[0] ||
#            '- mapStr.a[0]:' || st.mapStr.a[0] ||
#            '- mapInt.a[0]:' || st.mapInt.a[0] ||
#            '- recStr.fmap.a[0]:' || st.recStr.fmap.a[0] ||
#            '- recFlt.fmap.a[0]:' || st.recFlt.fmap.a[0] ||
#            '- arrrecStr[0].fmap.a[0]:' || st.arrrecStr[0].fmap.a[0] ||
#            '- arrrecFlt[0].fmap.a[0]:' || st.arrrecFlt[0].fmap.a[0]
#            as conc
#      from stringsTable2 st ORDER BY id

select id, st.str ||
           '- jsn.a1:' || st.jsn.a1 ||
           '- jsn.b1:' || st.jsn.b1 ||
           '- jsn.b1[]:' || st.jsn.b1[] ||
           '- jsn.c1:' || st.jsn.c1 ||
           '- jsn.c1[]:' || st.jsn.c1 ||
           '- jsn.c1[0]:' || st.jsn.c1[0] ||
           '- arrStr[0]:' || st.arrStr[0] ||
           '- arrStr[]:' || st.arrStr[] ||
           '- arrInt[0]:' || st.arrInt[0] ||
           '- arrInt[]:' || st.arrInt[] ||
           '- mapStr.a[0]:' || st.mapStr.a[0] ||
           '- mapStr.a[]:' || st.mapStr.a[] ||
           '- mapInt.a[0]:' || st.mapInt.a[0] ||
           '- mapInt.a[]:' || st.mapInt.a[] ||
           '- recStr.fmap.a[0]:' || st.recStr.fmap.a[0] ||
           '- recStr.fmap.a[]:' || st.recStr.fmap.a[] ||
           '- recFlt.fmap.a[0]:' || st.recFlt.fmap.a[0] ||
           '- recFlt.fmap.a[]:' || st.recFlt.fmap.a[] ||
           '- arrrecStr[0].fmap.a[0]:' || st.arrrecStr[0].fmap.a[0] ||
           '- arrrecStr[0].fmap.a[]:' || st.arrrecStr[0].fmap.a[] ||
           '- arrrecFlt[0].fmap.a[0]:' || st.arrrecFlt[0].fmap.a[0] ||
           '- arrrecFlt[0].fmap.a[]:' || st.arrrecFlt[0].fmap.a[]
           as conc
    from stringsTable2 st ORDER BY id
