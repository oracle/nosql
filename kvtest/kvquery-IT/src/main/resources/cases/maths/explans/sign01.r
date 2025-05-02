compiled-query-plan

{
"query file" : "maths/q/sign01.q",
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
        "field name" : "sign0",
        "field expression" : 
        {
          "iterator kind" : "SIGN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0
            }
          ]
        }
      },
      {
        "field name" : "sign1",
        "field expression" : 
        {
          "iterator kind" : "SIGN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "sign10",
        "field expression" : 
        {
          "iterator kind" : "SIGN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 1.0
            }
          ]
        }
      },
      {
        "field name" : "signneg1",
        "field expression" : 
        {
          "iterator kind" : "SIGN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -1
            }
          ]
        }
      },
      {
        "field name" : "signneg10",
        "field expression" : 
        {
          "iterator kind" : "SIGN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -1.0
            }
          ]
        }
      },
      {
        "field name" : "sign000001",
        "field expression" : 
        {
          "iterator kind" : "SIGN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 1.0E-6
            }
          ]
        }
      },
      {
        "field name" : "signneg000001",
        "field expression" : 
        {
          "iterator kind" : "SIGN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -1.0E-6
            }
          ]
        }
      },
      {
        "field name" : "signneg100",
        "field expression" : 
        {
          "iterator kind" : "SIGN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -100
            }
          ]
        }
      },
      {
        "field name" : "sign102234",
        "field expression" : 
        {
          "iterator kind" : "SIGN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 102234
            }
          ]
        }
      }
    ]
  }
}
}