#cos of nested complex types map in arrays and arrays in map and in json
select t.nestedNumMapInArray, cos(t.nestedNumMapInArray) as cosnestedNumMapInArray,
       t.nestedDouMapInArray, cos(t.nestedDouMapInArray) as cosnestedDouMapInArray,
       t.nestedNumArrayInMap, cos(t.nestedNumArrayInMap) as cosnestedNumArrayInMap,
       t.nestedDouArrayInMap, cos(t.nestedDouArrayInMap) as cosnestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, cos(t.doc.nestedNumMapInArray) as cosdocnestedNumMapInArray,
       t.doc.nestedDouMapInArray as docnestedDouMapInArray, cos(t.doc.nestedDouMapInArray) as cosdocnestedDouMapInArray,
       t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, cos(t.doc.nestedNumArrayInMap) as cosdocnestedNumArrayInMap,
       t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, cos(t.doc.nestedDouArrayInMap) as cosdocnestedDouArrayInMap
 from functional_test t where id=1