compiled-query-plan

{
"query file" : "maths/q/ceil09.q",
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
      "target table" : "math_test",
      "row variable" : "$$t",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":1},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Integer",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "ic",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
                }
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Long",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "lc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
                }
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_3",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Float",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "fc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
                }
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_4",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Double",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "dc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
                }
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_5",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Number",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "nc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
                }
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_6",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Integer",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "PROMOTE",
                "target type" : "Any",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "ic",
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
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_7",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Long",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "PROMOTE",
                "target type" : "Any",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "lc",
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
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_8",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Double",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "PROMOTE",
                "target type" : "Any",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "fc",
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
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_9",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Double",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "dc",
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
            ]
          }
        }
      },
      {
        "field name" : "Column_10",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Double",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "PROMOTE",
                "target type" : "Any",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "nc",
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
              }
            ]
          }
        }
      }
    ]
  }
}
}