compiled-query-plan

{
"query file" : "nested_arrays/q/q17.q",
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
      "target table" : "Foo",
      "row variable" : "$$f",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
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
        "input iterator" :
        {
          "iterator kind" : "ARRAY_FILTER",
          "predicate iterator" :
          {
            "iterator kind" : "OP_NOT_EXISTS",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_FILTER",
              "predicate iterator" :
              {
                "iterator kind" : "ANY_EQUAL",
                "left operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "areacode",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$element"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 500
                }
              },
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$element"
              }
            }
          },
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "phones",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "addresses",
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