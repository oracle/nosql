#ceil of nested complex types map in arrays and arrays in map and in json
select t.nestedNumMapInArray, ceil(t.nestedNumMapInArray) as ceilnestedNumMapInArray,
       t.nestedDouMapInArray, ceil(t.nestedDouMapInArray) as ceilnestedDouMapInArray,
       t.nestedNumArrayInMap, ceil(t.nestedNumArrayInMap) as ceilnestedNumArrayInMap,
       t.nestedDouArrayInMap, ceil(t.nestedDouArrayInMap) as ceilnestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, ceil(t.doc.nestedNumMapInArray) as ceildocnestedNumMapInArray,
       t.doc.nestedDouMapInArray as docnestedDouMapInArray, ceil(t.doc.nestedDouMapInArray) as ceildocnestedDouMapInArray,
       t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, ceil(t.doc.nestedNumArrayInMap) as ceildocnestedNumArrayInMap,
       t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, ceil(t.doc.nestedDouArrayInMap) as ceildocnestedDouArrayInMap
 from functional_test t where id=1