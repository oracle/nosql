select st.id,
    index_of(st.str1, '⏳') as i1,
    index_of(st.str2, '⏳') as i2,
    index_of(st.str3, '⏳') as i3,
    index_of('a😛bz😸c😛dz😸e', 'z😸') as z3,
    index_of('a😛bz😸c😛dz😸e', 'z😸', 4) as z8
from
    stringsTable st ORDER BY id