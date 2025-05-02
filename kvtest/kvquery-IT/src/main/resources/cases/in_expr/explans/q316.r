compiled-query-plan

{
"query file" : "in_expr/q/q316.q",
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
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"info.bar1":0,"info.bar2":0.0},
          "range conditions" : { "info.bar3" : { "start value" : "a", "start inclusive" : true, "end value" : "p", "end inclusive" : false } }
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "ARRAY_FILTER",
          "input iterator" :
          {
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$arr9"
          }
        },
        {
          "iterator kind" : "ARRAY_FILTER",
          "input iterator" :
          {
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$arr10"
          }
        }
      ],
      "map of key bind expressions" : [
        [ 0, 1, -1, -1 ]
      ],
      "bind info for in3 operator" : [
        {
          "theNumComps" : 1,
          "thePushedComps" : [ 0 ],
          "theIndexFieldPositions" : [ 0 ]
         },
        {
          "theNumComps" : 2,
          "thePushedComps" : [ 1 ],
          "theIndexFieldPositions" : [ 1 ]
         }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$f",
    "WHERE" : 
    {
      "iterator kind" : "IN",
      "left-hand-side expressions" : [
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "foo1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f"
          }
        },
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "bar2",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        }
      ],
      "right-hand-side expressions" : [
        {
          "iterator kind" : "ARRAY_FILTER",
          "input iterator" :
          {
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$arr10"
          }
        }
      ]
    },
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f"
          }
        }
      }
    ]
  }
}
}