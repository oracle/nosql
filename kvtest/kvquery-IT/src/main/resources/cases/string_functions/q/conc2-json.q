# '- jsn.a1:' || st.jsn.a1       - a string
# '- jsn.b1:' || st.jsn.b1       - an int
# '- jsn.b1[]:' || st.jsn.b1[]   - an int
# '- jsn.c1:' || st.jsn.c1       - a json array
# '- jsn.c1[]:' || st.jsn.c1[]   - a sequence of values
# '- jsn.c1[0]:' || st.jsn.c1[0] - a string

select id, st.str ||
           '- jsn.a1:' || st.jsn.a1 ||
           '- jsn.b1:' || st.jsn.b1 ||
           '- jsn.b1[]:' || st.jsn.b1[] ||
           '- jsn.c1:' || st.jsn.c1 ||
           '- jsn.c1[]:' || st.jsn.c1[] ||
           '- jsn.c1[0]:' || st.jsn.c1[0]
           as conc
    from stringsTable2 st ORDER BY id
