compiled-query-plan

{
"query file" : "idc_loj/q/q15.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-2",
  "input iterator" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_PARTITIONS",
    "input iterator" :
    {
      "iterator kind" : "GROUP",
      "input variable" : "$gb-1",
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "A.G",
          "row variable" : "$$g",
          "index used" : "primary index",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "ancestor tables" : [
            { "table" : "A", "row variable" : "$$a", "covering primary index" : true }          ],
          "descendant tables" : [
            { "table" : "A.G.H", "row variable" : "$$h", "covering primary index" : false }
          ],
          "position in join" : 0
        },
        "FROM variables" : ["$$a", "$$g", "$$h"],
        "WHERE" : 
        {
          "iterator kind" : "OR",
          "input iterators" : [
            {
              "iterator kind" : "EQUAL",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "h3",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$h"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : 9223372036854775807
              }
            },
            {
              "iterator kind" : "EQUAL",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "h3",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$h"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : -9223372036854775808
              }
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
            "field name" : "g_g2",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "g2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$g"
              }
            }
          },
          {
            "field name" : "h_h3",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "h3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$h"
              }
            }
          }
        ]
      },
      "grouping expressions" : [
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "a_ida1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-1"
          }
        },
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "g_g2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-1"
          }
        },
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "h_h3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-1"
          }
        }
      ],
      "aggregate functions" : [

      ]
    }
  },
  "grouping expressions" : [
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "a_ida1",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-2"
      }
    },
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "g_g2",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-2"
      }
    },
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "h_h3",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-2"
      }
    }
  ],
  "aggregate functions" : [

  ]
}
}