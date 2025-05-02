#abs of nested complex types map in arrays and arrays in map and in json (NaN's and nulls)
select t.nestedNumMapInArray, abs(t.nestedNumMapInArray) as absnestedNumMapInArray,
       t.nestedDouMapInArray, abs(t.nestedDouMapInArray) as absnestedDouMapInArray,
       t.nestedNumArrayInMap, abs(t.nestedNumArrayInMap) as absnestedNumArrayInMap,
       t.nestedDouArrayInMap, abs(t.nestedDouArrayInMap) as absnestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, abs(t.doc.nestedNumMapInArray) as absdocnestedNumMapInArray,
              t.doc.nestedDouMapInArray as docnestedDouMapInArray, abs(t.doc.nestedDouMapInArray) as absdocnestedDouMapInArray,
              t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, abs(t.doc.nestedNumArrayInMap) as absdocnestedNumArrayInMap,
              t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, abs(t.doc.nestedDouArrayInMap) as absdocnestedDouArrayInMap
 from functional_test t where id=4
