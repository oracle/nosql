compiled-query-plan

{
"query file" : "idc_in_expr/q/q17.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "SimpleDatatype",
      "row variable" : "$$simpleDatatype",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":0},
          "range conditions" : {}
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "CONST",
          "value" : "Shyam"
        }
      ],
      "map of key bind expressions" : [
        [ 0 ]
      ],
      "bind info for in3 operator" : [
        {
          "theNumComps" : 1,
          "thePushedComps" : [ 0 ],
          "theIndexFieldPositions" : [ 0 ]
         }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$simpleDatatype",
    "SELECT expressions" : [
      {
        "field name" : "simpleDatatype",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$simpleDatatype"
        }
      }
    ]
  }
}
}