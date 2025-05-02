compiled-query-plan

{
"query file" : "in_expr/q/q305.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "foo",
      "row variable" : "$$f",
      "index used" : "idx_bar1234",
      "covering index" : true,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {"info.bar1":0,"info.bar2":0.0,"info.bar3":""},
          "range conditions" : { "info.bar4" : { "start value" : 101, "start inclusive" : true, "end value" : 108, "end inclusive" : false } }
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "ARRAY_FILTER",
          "input iterator" :
          {
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$arr7"
          }
        },
        {
          "iterator kind" : "ARRAY_FILTER",
          "input iterator" :
          {
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$arr8"
          }
        }
      ],
      "map of key bind expressions" : [
        [ 0, 1, 0, -1, -1 ]
      ],
      "bind info for in3 operator" : [
        {
          "theNumComps" : 2,
          "thePushedComps" : [ 0, 1 ],
          "theIndexFieldPositions" : [ 0, 2 ]
         },
        {
          "theNumComps" : 1,
          "thePushedComps" : [ 0 ],
          "theIndexFieldPositions" : [ 1 ]
         }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$f_idx",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f_idx"
          }
        }
      }
    ]
  }
}
}