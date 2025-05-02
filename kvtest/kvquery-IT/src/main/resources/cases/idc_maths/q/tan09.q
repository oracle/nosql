#tan of maps and nested map and in json values
select t.numMap, tan(t.numMap) as tannumMap,
       t.numNestedMap, tan(t.numNestedMap) as tannumNestedMap,
       t.douMap, tan(t.douMap) as tandouMap,
       t.douNestedMap, tan(t.douNestedMap) as tandouNestedMap,
       t.doc.numMap as docnumMap, tan(t.doc.numMap) as tandocnumMap,
       t.doc.numNestedMap as docnumNestedMap, tan(t.doc.numNestedMap) as tandocnumNestedMap,
       t.doc.douMap as docdouMap, tan(t.doc.douMap) as tandocdouMap,
       t.doc.douNestedMap as docdouNestedMap, tan(t.doc.douNestedMap) as tandocdouNestedMap
 from functional_test t where id=1