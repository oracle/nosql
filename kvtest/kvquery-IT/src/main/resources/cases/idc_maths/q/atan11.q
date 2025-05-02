#atan of nested complex types map in arrays and arrays in map and in json
select t.nestedNumMapInArray, atan(t.nestedNumMapInArray) as atannestedNumMapInArray,
       t.nestedDouMapInArray, atan(t.nestedDouMapInArray) as atannestedDouMapInArray,
       t.nestedNumArrayInMap, atan(t.nestedNumArrayInMap) as atannestedNumArrayInMap,
       t.nestedDouArrayInMap, atan(t.nestedDouArrayInMap) as atannestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, atan(t.doc.nestedNumMapInArray) as atandocnestedNumMapInArray,
       t.doc.nestedDouMapInArray as docnestedDouMapInArray, atan(t.doc.nestedDouMapInArray) as atandocnestedDouMapInArray,
       t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, atan(t.doc.nestedNumArrayInMap) as atandocnestedNumArrayInMap,
       t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, atan(t.doc.nestedDouArrayInMap) as atandocnestedDouArrayInMap
 from functional_test t where id=1