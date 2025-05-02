#acos of nested complex types map in arrays and arrays in map and in json
select t.nestedNumMapInArray, acos(t.nestedNumMapInArray) as acosnestedNumMapInArray,
       t.nestedDouMapInArray, acos(t.nestedDouMapInArray) as acosnestedDouMapInArray,
       t.nestedNumArrayInMap, acos(t.nestedNumArrayInMap) as acosnestedNumArrayInMap,
       t.nestedDouArrayInMap, acos(t.nestedDouArrayInMap) as acosnestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, acos(t.doc.nestedNumMapInArray) as acosdocnestedNumMapInArray,
       t.doc.nestedDouMapInArray as docnestedDouMapInArray, acos(t.doc.nestedDouMapInArray) as acosdocnestedDouMapInArray,
       t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, acos(t.doc.nestedNumArrayInMap) as acosdocnestedNumArrayInMap,
       t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, acos(t.doc.nestedDouArrayInMap) as acosdocnestedDouArrayInMap
 from functional_test t where id=1