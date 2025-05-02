compiled-query-plan

{
"query file" : "idc_loj/q/q16.q",
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
      "target table" : "A",
      "row variable" : "$$a",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "descendant tables" : [
        { "table" : "A.B", "row variable" : "$$b", "covering primary index" : true }
      ],
      "position in join" : 0
    },
    "FROM variables" : ["$$a", "$$b"],
    "WHERE" : 
    {
      "iterator kind" : "AND",
      "input iterators" : [
        {
          "iterator kind" : "OP_IS_NOT_NULL",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "a2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$a"
            }
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
                "field name" : "a2",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$a"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : 2147483647
              }
            },
            {
              "iterator kind" : "EQUAL",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "a2",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$a"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : -2147483648
              }
            }
          ]
        }
      ]
    },
    "SELECT expressions" : [
      {
        "field name" : "a_ida1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$a"
          }
        }
      },
      {
        "field name" : "a_a2",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "a2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$a"
          }
        }
      },
      {
        "field name" : "a_a3",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "a3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$a"
          }
        }
      }
    ]
  }
}
}