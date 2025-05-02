compiled-query-plan

{
"query file" : "idc_maths/q/log01.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "functional_test",
      "row variable" : "$$t",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":4},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "SELECT expressions" : [
      {
        "field name" : "log_null_10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "iv",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_10_10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_null_null",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "lv",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                },
                {
                  "iterator kind" : "CONST",
                  "value" : null
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_NaN_null",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "fv",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                },
                {
                  "iterator kind" : "CONST",
                  "value" : null
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_NaN_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "dv",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_null_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "PROMOTE",
                  "target type" : "Any",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "iv",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "doc",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$t"
                      }
                    }
                  }
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_NaN_inf",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "PROMOTE",
                  "target type" : "Any",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "fv",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "doc",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$t"
                      }
                    }
                  }
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1.7976931348623157E308
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_NaN_neginf",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "PROMOTE",
                  "target type" : "Any",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "dv",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "doc",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$t"
                      }
                    }
                  }
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -1.7976931348623157E308
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_null_inf",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "numArr",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1.7976931348623157E308
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_null_neginf",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "PROMOTE",
                  "target type" : "Any",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "numArr",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "doc",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$t"
                      }
                    }
                  }
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -1.7976931348623157E308
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      }
    ]
  }
}
}