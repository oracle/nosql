#ln of maps and nested map and in json values
select t.numMap, ln(t.numMap) as lnnumMap,
       t.numNestedMap, ln(t.numNestedMap) as lnnumNestedMap,
       t.douMap, ln(t.douMap) as lndouMap,
       t.douNestedMap, ln(t.douNestedMap) as lndouNestedMap,
       t.doc.numMap as docnumMap, ln(t.doc.numMap) as lndocnumMap,
       t.doc.numNestedMap as docnumNestedMap, ln(t.doc.numNestedMap) as lndocnumNestedMap,
       t.doc.douMap as docdouMap, ln(t.doc.douMap) as lndocdouMap,
       t.doc.douNestedMap as docdouNestedMap, ln(t.doc.douNestedMap) as lndocdouNestedMap
 from functional_test t where id=1