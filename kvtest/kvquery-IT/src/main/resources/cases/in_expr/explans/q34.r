compiled-query-plan

{
"query file" : "in_expr/q/q34.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "distinct by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "foo",
      "row variable" : "$$f",
      "index used" : "idx_phones",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"info.phones[].num":3},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.phones[].num":5},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$f",
    "WHERE" : 
    {
      "iterator kind" : "OP_EXISTS",
      "input iterator" :
      {
        "iterator kind" : "ARRAY_FILTER",
        "predicate iterator" :
        {
          "iterator kind" : "IN",
          "left-hand-side expressions" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "kind",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$element"
              }
            }
          ],
          "right-hand-side expressions" : [
            {
              "iterator kind" : "CONST",
              "value" : "a"
            },
            {
              "iterator kind" : "CONST",
              "value" : "b"
            }
          ]
        },
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "phones",
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
      }
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