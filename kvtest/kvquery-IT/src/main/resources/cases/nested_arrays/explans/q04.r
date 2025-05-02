compiled-query-plan

{
"query file" : "nested_arrays/q/q04.q",
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
      "target table" : "Foo",
      "row variable" : "$$f",
      "index used" : "idx_areacode_number_long",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"info.addresses[].phones[][][].areacode":408},
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
          "iterator kind" : "AND",
          "input iterators" : [
            {
              "iterator kind" : "EQUAL",
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
                "value" : 408
              }
            },
            {
              "iterator kind" : "EQUAL",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "kind",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$element"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : "work"
              }
            }
          ]
        },
        "input iterator" :
        {
          "iterator kind" : "ARRAY_FILTER",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_FILTER",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "phones",
              "input iterator" :
              {
                "iterator kind" : "ARRAY_FILTER",
                "predicate iterator" :
                {
                  "iterator kind" : "EQUAL",
                  "left operand" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "state",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$element"
                    }
                  },
                  "right operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : "CA"
                  }
                },
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