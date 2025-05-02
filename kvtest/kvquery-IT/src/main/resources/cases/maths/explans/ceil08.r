compiled-query-plan

{
"query file" : "maths/q/ceil08.q",
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
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 7.8
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 8.0
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : -7.8
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : -7.0
          }
        }
      },
      {
        "field name" : "Column_3",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 0
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 0
          }
        }
      },
      {
        "field name" : "Column_4",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 0.0
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 0.0
          }
        }
      },
      {
        "field name" : "Column_5",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 9.99999999999E9
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 1.0E10
          }
        }
      },
      {
        "field name" : "Column_6",
        "field expression" : 
        {
          "iterator kind" : "OP_IS_NULL",
          "input iterator" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : null
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_7",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "CEIL",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 1234567890.123456789
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 1234567891
          }
        }
      },
      {
        "field name" : "Column_8",
        "field expression" : 
        {
          "iterator kind" : "CEIL",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -0.2
            }
          ]
        }
      }
    ]
  }
}
}