compiled-query-plan

{
"query file" : "multi_index/q/foo9.q",
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
      "target table" : "Foo",
      "row variable" : "$$t",
      "index used" : "idx_id3",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id3":0},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "WHERE" : 
    {
      "iterator kind" : "GREATER_THAN",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "f",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "rec",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 0.0
      }
    },
    "SELECT expressions" : [
      {
        "field name" : "id1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "f",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "f",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "rec",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      }
    ]
  }
}
}