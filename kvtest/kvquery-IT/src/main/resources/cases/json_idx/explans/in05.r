compiled-query-plan

{
"query file" : "json_idx/q/in05.q",
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
      "index used" : "idx_children_both",
      "covering index" : false,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "ANY_LESS_OR_EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info.children.values().school",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : "sch_1"
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$f",
    "WHERE" : 
    {
      "iterator kind" : "ANY_LESS_OR_EQUAL",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "school",
        "input iterator" :
        {
          "iterator kind" : "VALUES",
          "predicate iterator" :
          {
            "iterator kind" : "AND",
            "input iterators" : [
              {
                "iterator kind" : "IN",
                "left-hand-side expressions" : [
                  {
                    "iterator kind" : "FN_LTRIM",
                    "input iterators" : [
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$key"
                      }
                    ]
                  }
                ],
                "right-hand-side expressions" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : "Anna"
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : "Mark"
                  }
                ]
              },
              {
                "iterator kind" : "IN",
                "left-hand-side expressions" : [
                  {
                    "iterator kind" : "ADD_SUBTRACT",
                    "operations and operands" : [
                      {
                        "operation" : "+",
                        "operand" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "age",
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$value"
                          }
                        }
                      },
                      {
                        "operation" : "+",
                        "operand" :
                        {
                          "iterator kind" : "CONST",
                          "value" : 1
                        }
                      }
                    ]
                  }
                ],
                "right-hand-side expressions" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : 3
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : 5
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : 10
                  }
                ]
              }
            ]
          },
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "children",
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
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : "sch_1"
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