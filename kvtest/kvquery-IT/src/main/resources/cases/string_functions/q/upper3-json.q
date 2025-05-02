# st.arrStr[] returns all items in the array as a sequence,
# seq_transform() applies upper() function to each string in the array
# individually and then make a array for the result using array constructor []

select id, upper(st.arrStr) as up_arrStr,
           [seq_transform(st.arrStr[], upper($))] as seq_tr_arrStr
from stringsTable2 st ORDER BY id