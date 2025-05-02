#floor of nested complex types map in arrays and arrays in map and in json
select t.nestedNumMapInArray, floor(t.nestedNumMapInArray) as floornestedNumMapInArray,
       t.nestedDouMapInArray, floor(t.nestedDouMapInArray) as floornestedDouMapInArray,
       t.nestedNumArrayInMap, floor(t.nestedNumArrayInMap) as floornestedNumArrayInMap,
       t.nestedDouArrayInMap, floor(t.nestedDouArrayInMap) as floornestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, floor(t.doc.nestedNumMapInArray) as floordocnestedNumMapInArray,
       t.doc.nestedDouMapInArray as docnestedDouMapInArray, floor(t.doc.nestedDouMapInArray) as floordocnestedDouMapInArray,
       t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, floor(t.doc.nestedNumArrayInMap) as floordocnestedNumArrayInMap,
       t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, floor(t.doc.nestedDouArrayInMap) as floordocnestedDouArrayInMap
 from functional_test t where id=1