#degrees of nested complex types map in arrays and arrays in map and in json
select t.nestedNumMapInArray, degrees(t.nestedNumMapInArray) as degreesnestedNumMapInArray,
       t.nestedDouMapInArray, degrees(t.nestedDouMapInArray) as degreesnestedDouMapInArray,
       t.nestedNumArrayInMap, degrees(t.nestedNumArrayInMap) as degreesnestedNumArrayInMap,
       t.nestedDouArrayInMap, degrees(t.nestedDouArrayInMap) as degreesnestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, degrees(t.doc.nestedNumMapInArray) as degreesdocnestedNumMapInArray,
       t.doc.nestedDouMapInArray as docnestedDouMapInArray, degrees(t.doc.nestedDouMapInArray) as degreesdocnestedDouMapInArray,
       t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, degrees(t.doc.nestedNumArrayInMap) as degreesdocnestedNumArrayInMap,
       t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, degrees(t.doc.nestedDouArrayInMap) as degreesdocnestedDouArrayInMap
 from functional_test t where id=1