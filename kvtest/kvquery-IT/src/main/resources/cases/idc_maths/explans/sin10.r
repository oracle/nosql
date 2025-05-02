compiled-query-plan

{
"query file" : "idc_maths/q/sin10.q",
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
          "equality conditions" : {"id":1},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "SELECT expressions" : [
      {
        "field name" : "nestedNumMapInArray",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "nestedNumMapInArray",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "sinnestedNumMapInArray",
        "field expression" : 
        {
          "iterator kind" : "SIN",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "nestedNumMapInArray",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          ]
        }
      },
      {
        "field name" : "nestedDouMapInArray",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "nestedDouMapInArray",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "sinnestedDouMapInArray",
        "field expression" : 
        {
          "iterator kind" : "SIN",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "nestedDouMapInArray",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          ]
        }
      },
      {
        "field name" : "nestedNumArrayInMap",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "nestedNumArrayInMap",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "sinnestedNumArrayInMap",
        "field expression" : 
        {
          "iterator kind" : "SIN",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "nestedNumArrayInMap",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          ]
        }
      },
      {
        "field name" : "nestedDouArrayInMap",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "nestedDouArrayInMap",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "sinnestedDouArrayInMap",
        "field expression" : 
        {
          "iterator kind" : "SIN",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "nestedDouArrayInMap",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          ]
        }
      },
      {
        "field name" : "docnestedNumMapInArray",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "nestedNumMapInArray",
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
      },
      {
        "field name" : "sindocnestedNumMapInArray",
        "field expression" : 
        {
          "iterator kind" : "SIN",
          "input iterators" : [
            {
              "iterator kind" : "PROMOTE",
              "target type" : "Any",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "nestedNumMapInArray",
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
      },
      {
        "field name" : "docnestedDouMapInArray",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "nestedDouMapInArray",
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
      },
      {
        "field name" : "sindocnestedDouMapInArray",
        "field expression" : 
        {
          "iterator kind" : "SIN",
          "input iterators" : [
            {
              "iterator kind" : "PROMOTE",
              "target type" : "Any",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "nestedDouMapInArray",
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
      },
      {
        "field name" : "docnestedNumArrayInMap",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "nestedNumArrayInMap",
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
      },
      {
        "field name" : "sindocnestedNumArrayInMap",
        "field expression" : 
        {
          "iterator kind" : "SIN",
          "input iterators" : [
            {
              "iterator kind" : "PROMOTE",
              "target type" : "Any",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "nestedNumArrayInMap",
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
      },
      {
        "field name" : "docnestedDouArrayInMap",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "nestedDouArrayInMap",
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
      },
      {
        "field name" : "sindocnestedDouArrayInMap",
        "field expression" : 
        {
          "iterator kind" : "SIN",
          "input iterators" : [
            {
              "iterator kind" : "PROMOTE",
              "target type" : "Any",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "nestedDouArrayInMap",
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
    ]
  }
}
}