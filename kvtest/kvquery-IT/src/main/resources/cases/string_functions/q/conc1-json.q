select id, st.str ||
           '- jsn:' || st.jsn as conc
    from stringsTable2 st ORDER BY id
