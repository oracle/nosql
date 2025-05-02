compiled-query-plan

{
"query file" : "maths/q/abs03.q",
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
          "equality conditions" : {"id":2},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "SELECT expressions" : [
      {
        "field name" : "ic",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ic",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "absic",
        "field expression" : 
        {
          "iterator kind" : "ABS",
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
      },
      {
        "field name" : "lc",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "lc",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "abslc",
        "field expression" : 
        {
          "iterator kind" : "ABS",
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
      },
      {
        "field name" : "fc",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "fc",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "absfc",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ABS",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "dc",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "dc",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "absdc",
        "field expression" : 
        {
          "iterator kind" : "ABS",
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
      },
      {
        "field name" : "nc",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "nc",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "absnc",
        "field expression" : 
        {
          "iterator kind" : "ABS",
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
    ]
  }
}
}