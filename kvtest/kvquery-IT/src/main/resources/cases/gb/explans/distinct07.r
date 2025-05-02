compiled-query-plan

{
"query file" : "gb/q/distinct07.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-0",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "SORT",
      "order by fields at positions" : [ 2, 1 ],
      "input iterator" :
      {
        "iterator kind" : "RECEIVE",
        "distribution kind" : "SINGLE_PARTITION",
        "input iterator" :
        {
          "iterator kind" : "SELECT",
          "FROM" :
          {
            "iterator kind" : "TABLE",
            "target table" : "Foo",
            "row variable" : "$$f",
            "index used" : "idx_acc_year_prodcat",
            "covering index" : false,
            "index row variable" : "$$f_idx",
            "index scans" : [
              {
                "equality conditions" : {},
                "range conditions" : { "xact.acctno" : { "start value" : 345, "start inclusive" : false, "end value" : 500, "end inclusive" : false } }
              }
            ],
            "index filtering predicate" :
            {
              "iterator kind" : "AND",
              "input iterators" : [
                {
                  "iterator kind" : "EQUAL",
                  "left operand" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "#id1",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$f_idx"
                    }
                  },
                  "right operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 0
                  }
                },
                {
                  "iterator kind" : "OR",
                  "input iterators" : [
                    {
                      "iterator kind" : "EQUAL",
                      "left operand" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "xact.year",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$$f_idx"
                        }
                      },
                      "right operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : null
                      }
                    },
                    {
                      "iterator kind" : "AND",
                      "input iterators" : [
                        {
                          "iterator kind" : "LESS_THAN",
                          "left operand" :
                          {
                            "iterator kind" : "CONST",
                            "value" : 1990
                          },
                          "right operand" :
                          {
                            "iterator kind" : "FIELD_STEP",
                            "field name" : "xact.year",
                            "input iterator" :
                            {
                              "iterator kind" : "VAR_REF",
                              "variable" : "$$f_idx"
                            }
                          }
                        },
                        {
                          "iterator kind" : "LESS_THAN",
                          "left operand" :
                          {
                            "iterator kind" : "FIELD_STEP",
                            "field name" : "xact.year",
                            "input iterator" :
                            {
                              "iterator kind" : "VAR_REF",
                              "variable" : "$$f_idx"
                            }
                          },
                          "right operand" :
                          {
                            "iterator kind" : "CONST",
                            "value" : 2020
                          }
                        }
                      ]
                    }
                  ]
                }
              ]
            },
            "position in join" : 0
          },
          "FROM variable" : "$$f",
          "SELECT expressions" : [
            {
              "field name" : "long",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "long",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "record",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f"
                  }
                }
              }
            },
            {
              "field name" : "int",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "int",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "record",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f"
                  }
                }
              }
            },
            {
              "field name" : "sort_gen",
              "field expression" : 
              {
                "iterator kind" : "ADD_SUBTRACT",
                "operations and operands" : [
                  {
                    "operation" : "+",
                    "operand" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "int",
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "record",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$$f"
                        }
                      }
                    }
                  },
                  {
                    "operation" : "+",
                    "operand" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "long",
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "record",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$$f"
                        }
                      }
                    }
                  }
                ]
              }
            }
          ]
        }
      }
    },
    "FROM variable" : "$from-1",
    "SELECT expressions" : [
      {
        "field name" : "long",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "long",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      },
      {
        "field name" : "int",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "int",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    ]
  },
  "grouping expressions" : [
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "long",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-0"
      }
    },
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "int",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-0"
      }
    }
  ],
  "aggregate functions" : [

  ]
}
}