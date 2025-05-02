compiled-query-plan

{
"query file" : "nested_arrays/q/q52.q",
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
      "row variable" : "$$t",
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
    "FROM variable" : "$$t",
    "WHERE" : 
    {
      "iterator kind" : "IS_OF_TYPE",
      "target types" : [
        {
        "type" : { "Array" : 
          "Any"
        },
        "quantifier" : "+",
        "only" : false
        }
      ],
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
              "variable" : "$$t"
            }
          }
        }
      }
    },
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "MAP_CONSTRUCTOR",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "id"
            },
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "phones"
            },
            {
              "iterator kind" : "ARRAY_CONSTRUCTOR",
              "conditional" : false,
              "input iterators" : [
                {
                  "iterator kind" : "SEQ_MAP",
                  "mapper iterator" :
                  {
                    "iterator kind" : "MAP_CONSTRUCTOR",
                    "input iterators" : [
                      {
                        "iterator kind" : "CONST",
                        "value" : "num_phones"
                      },
                      {
                        "iterator kind" : "FUNC_SIZE",
                        "input iterator" :
                        {
                          "iterator kind" : "PROMOTE",
                          "target type" : "Any",
                          "input iterator" :
                          {
                            "iterator kind" : "FIELD_STEP",
                            "field name" : "phones",
                            "input iterator" :
                            {
                              "iterator kind" : "VAR_REF",
                              "variable" : "$sq1"
                            }
                          }
                        }
                      }
                    ]
                  },
                  "input iterator" :
                  {
                    "iterator kind" : "ARRAY_FILTER",
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
                          "variable" : "$$t"
                        }
                      }
                    }
                  }
                }
              ]
            }
          ]
        }
      }
    ]
  }
}
}