#sign of nested complex types map in arrays and arrays in map and in json
select t.nestedNumMapInArray, sign(t.nestedNumMapInArray) as signnestedNumMapInArray,
       t.nestedDouMapInArray, sign(t.nestedDouMapInArray) as signnestedDouMapInArray,
       t.nestedNumArrayInMap, sign(t.nestedNumArrayInMap) as signnestedNumArrayInMap,
       t.nestedDouArrayInMap, sign(t.nestedDouArrayInMap) as signnestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, sign(t.doc.nestedNumMapInArray) as signdocnestedNumMapInArray,
       t.doc.nestedDouMapInArray as docnestedDouMapInArray, sign(t.doc.nestedDouMapInArray) as signdocnestedDouMapInArray,
       t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, sign(t.doc.nestedNumArrayInMap) as signdocnestedNumArrayInMap,
       t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, sign(t.doc.nestedDouArrayInMap) as signdocnestedDouArrayInMap
 from functional_test t where id=1