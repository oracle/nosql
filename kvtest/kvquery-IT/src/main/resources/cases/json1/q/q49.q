select seq_transform(b.info[],
                     {
                       "elem1" : $.array[0],
                       "elem2" : $.array[1]
                     } )  as seq
FROM boo b
