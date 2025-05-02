compiled-query-plan

{
"query file" : "in_expr/q/q339.q",
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
          "equality conditions" : {"info.bar1":6,"info.bar2":0.0},
          "range conditions" : { "info.bar3" : { "end value" : "EMPTY", "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":6,"info.bar2":0.0},
          "range conditions" : { "info.bar3" : { "start value" : "EMPTY", "start inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":3,"info.bar2":0.0},
          "range conditions" : { "info.bar3" : { "end value" : "EMPTY", "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":3,"info.bar2":0.0},
          "range conditions" : { "info.bar3" : { "start value" : "EMPTY", "start inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":0.0},
          "range conditions" : { "info.bar3" : { "end value" : "EMPTY", "end inclusive" : false } }
        },
        {
          "equality conditions" : {"info.bar1":null,"info.bar2":0.0},
          "range conditions" : { "info.bar3" : { "start value" : "EMPTY", "start inclusive" : false } }
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "ARRAY_FILTER",
          "input iterator" :
          {
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$arr4"
          }
        }
      ],
      "map of key bind expressions" : [
        [ -1, 0, -1, -1 ],
        [ -1, 0, -1, -1 ],
        [ -1, 0, -1, -1 ],
        [ -1, 0, -1, -1 ],
        [ -1, 0, -1, -1 ],
        [ -1, 0, -1, -1 ]
      ],
      "bind info for in3 operator" : [
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