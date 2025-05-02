compiled-query-plan

{
"query file" : "in_expr/q/q345.q",
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
      "target table" : "foo",
      "row variable" : "$$foo",
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
          "iterator kind" : "SEQ_CONCAT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 1
            },
            {
              "iterator kind" : "CONST",
              "value" : 2
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
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
    "FROM variable" : "$$foo",
    "SELECT expressions" : [
      {
        "field name" : "foo",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$foo"
        }
      }
    ]
  }
}
}