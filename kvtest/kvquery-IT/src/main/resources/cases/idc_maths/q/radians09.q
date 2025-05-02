#radians of maps and nested map and in json values
select t.numMap, radians(t.numMap) as radiansnumMap,
       t.numNestedMap, radians(t.numNestedMap) as radiansnumNestedMap,
       t.douMap, radians(t.douMap) as radiansdouMap,
       t.douNestedMap, radians(t.douNestedMap) as radiansdouNestedMap,
       t.doc.numMap as docnumMap, radians(t.doc.numMap) as radiansdocnumMap,
       t.doc.numNestedMap as docnumNestedMap, radians(t.doc.numNestedMap) as radiansdocnumNestedMap,
       t.doc.douMap as docdouMap, radians(t.doc.douMap) as radiansdocdouMap,
       t.doc.douNestedMap as docdouNestedMap, radians(t.doc.douNestedMap) as radiansdocdouNestedMap
 from functional_test t where id=1