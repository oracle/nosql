#floor of maps and nested map and in json values
select t.numMap, floor(t.numMap) as floornumMap,
       t.numNestedMap, floor(t.numNestedMap) as floornumNestedMap,
       t.douMap, floor(t.douMap) as floordouMap,
       t.douNestedMap, floor(t.douNestedMap) as floordouNestedMap,
       t.doc.numMap as docnumMap, floor(t.doc.numMap) as floordocnumMap,
       t.doc.numNestedMap as docnumNestedMap, floor(t.doc.numNestedMap) as floordocnumNestedMap,
       t.doc.douMap as docdouMap, floor(t.doc.douMap) as floordocdouMap,
       t.doc.douNestedMap as docdouNestedMap, floor(t.doc.douNestedMap) as floordocdouNestedMap
 from functional_test t where id=1