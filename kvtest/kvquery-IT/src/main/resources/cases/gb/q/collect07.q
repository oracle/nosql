select array_collect({ "id1" : f.id1,
                       "id2" : f.id2,
                       "id3" : f.id3,
                       "prodcat" : f.xact.prodcat,
                       "year" : f.xact.year,
                       "str1" : "1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                       "str2" : "2xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                       "str3" : "3xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                       "str4" : "4xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                     }) as collect,
       count(*) as cnt
from Foo f
