#abs of maps and nested map and in json values (0's and infinities)
select t.numMap, abs(t.numMap) as absnumMap,
       t.numNestedMap, abs(t.numNestedMap) as absnumNestedMap,
       t.douMap, abs(t.douMap) as absdouMap,
       t.douNestedMap, abs(t.douNestedMap) as absdouNestedMap,
              t.doc.numMap as docnumMap, abs(t.doc.numMap) as absdocnumMap,
              t.doc.numNestedMap as docnumNestedMap, abs(t.doc.numNestedMap) as absdocnumNestedMap,
              t.doc.douMap as docdouMap, abs(t.doc.douMap) as absdocdouMap,
              t.doc.douNestedMap as docdouNestedMap, abs(t.doc.douNestedMap) as absdocdouNestedMap
 from functional_test t where id=3