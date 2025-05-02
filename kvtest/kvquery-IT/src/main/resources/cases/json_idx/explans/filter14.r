compiled-query-plan

{
"query file" : "json_idx/q/filter14.q",
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
      "index used" : "idx_areacode_kind",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"info.address.phones[].areacode":408},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$f",
    "WHERE" : 
    {
      "iterator kind" : "ANY_GREATER_THAN",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "areacode",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "phones",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_FILTER",
            "predicate iterator" :
            {
              "iterator kind" : "AND",
              "input iterators" : [
                {
                  "iterator kind" : "ANY_EQUAL",
                  "left operand" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "areacode",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "phones",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$element"
                      }
                    }
                  },
                  "right operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 408
                  }
                },
                {
                  "iterator kind" : "ANY_EQUAL",
                  "left operand" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "areacode",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "phones",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$element"
                      }
                    }
                  },
                  "right operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 650
                  }
                },
                {
                  "iterator kind" : "OP_EXISTS",
                  "input iterator" :
                  {
                    "iterator kind" : "ARRAY_FILTER",
                    "predicate iterator" :
                    {
                      "iterator kind" : "LESS_THAN",
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
                        "value" : 800
                      }
                    },
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "phones",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$element"
                      }
                    }
                  }
                }
              ]
            },
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "address",
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
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 510
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