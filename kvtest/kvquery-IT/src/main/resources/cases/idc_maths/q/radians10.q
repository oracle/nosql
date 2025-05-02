#radians of nested complex types map in arrays and arrays in map and in json
select t.nestedNumMapInArray, radians(t.nestedNumMapInArray) as radiansnestedNumMapInArray,
       t.nestedDouMapInArray, radians(t.nestedDouMapInArray) as radiansnestedDouMapInArray,
       t.nestedNumArrayInMap, radians(t.nestedNumArrayInMap) as radiansnestedNumArrayInMap,
       t.nestedDouArrayInMap, radians(t.nestedDouArrayInMap) as radiansnestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, radians(t.doc.nestedNumMapInArray) as radiansdocnestedNumMapInArray,
       t.doc.nestedDouMapInArray as docnestedDouMapInArray, radians(t.doc.nestedDouMapInArray) as radiansdocnestedDouMapInArray,
       t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, radians(t.doc.nestedNumArrayInMap) as radiansdocnestedNumArrayInMap,
       t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, radians(t.doc.nestedDouArrayInMap) as radiansdocnestedDouArrayInMap
 from functional_test t where id=1