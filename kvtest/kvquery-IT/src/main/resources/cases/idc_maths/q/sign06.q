#sign of maps and nested map and in json values
select t.numMap, sign(t.numMap) as signnumMap,
       t.numNestedMap, sign(t.numNestedMap) as signnumNestedMap,
       t.douMap, sign(t.douMap) as vdouMap,
       t.douNestedMap, sign(t.douNestedMap) as signdouNestedMap,
       t.doc.numMap as docnumMap, sign(t.doc.numMap) as signdocnumMap,
       t.doc.numNestedMap as docnumNestedMap, sign(t.doc.numNestedMap) as signdocnumNestedMap,
       t.doc.douMap as docdouMap, sign(t.doc.douMap) as signdocdouMap,
       t.doc.douNestedMap as docdouNestedMap, sign(t.doc.douNestedMap) as signdocdouNestedMap
 from functional_test t where id=1