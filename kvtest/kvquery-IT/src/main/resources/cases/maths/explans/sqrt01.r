compiled-query-plan

{
"query file" : "maths/q/sqrt01.q",
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
      "row variable" : "$$math_test",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {"id":1},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$math_test",
    "SELECT expressions" : [
      {
        "field name" : "sqrt0",
        "field expression" : 
        {
          "iterator kind" : "SQRT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0
            }
          ]
        }
      },
      {
        "field name" : "sqrt1",
        "field expression" : 
        {
          "iterator kind" : "SQRT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "sqrt4",
        "field expression" : 
        {
          "iterator kind" : "SQRT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "sqrt9",
        "field expression" : 
        {
          "iterator kind" : "SQRT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 9
            }
          ]
        }
      },
      {
        "field name" : "sqrt25",
        "field expression" : 
        {
          "iterator kind" : "SQRT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 25
            }
          ]
        }
      },
      {
        "field name" : "sqrt100",
        "field expression" : 
        {
          "iterator kind" : "SQRT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 100
            }
          ]
        }
      },
      {
        "field name" : "sqrt1000000000000",
        "field expression" : 
        {
          "iterator kind" : "SQRT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 1.0E12
            }
          ]
        }
      },
      {
        "field name" : "sqrt01",
        "field expression" : 
        {
          "iterator kind" : "SQRT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0.1
            }
          ]
        }
      },
      {
        "field name" : "sqrt05",
        "field expression" : 
        {
          "iterator kind" : "SQRT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0.5
            }
          ]
        }
      },
      {
        "field name" : "sqrt025",
        "field expression" : 
        {
          "iterator kind" : "SQRT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0.25
            }
          ]
        }
      },
      {
        "field name" : "aqrt064",
        "field expression" : 
        {
          "iterator kind" : "SQRT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0.64
            }
          ]
        }
      },
      {
        "field name" : "sqrt123045",
        "field expression" : 
        {
          "iterator kind" : "SQRT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 123.45
            }
          ]
        }
      },
      {
        "field name" : "sqrtneg1",
        "field expression" : 
        {
          "iterator kind" : "SQRT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -1
            }
          ]
        }
      },
      {
        "field name" : "sqrtneg0001",
        "field expression" : 
        {
          "iterator kind" : "SQRT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -0.001
            }
          ]
        }
      },
      {
        "field name" : "sqrtneg1024",
        "field expression" : 
        {
          "iterator kind" : "SQRT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -1024.0
            }
          ]
        }
      }
    ]
  }
}
}