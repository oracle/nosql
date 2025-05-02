compiled-query-plan

{
"query file" : "idc_unnest_json/q/arr05.q",
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
      "target table" : "User_json",
      "row variable" : "$u",
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
    "FROM variable" : "$u",
    "FROM" :
    {
      "iterator kind" : "ARRAY_FILTER",
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
              "variable" : "$u"
            }
          }
        }
      }
    },
    "FROM variable" : "$phone",
    "WHERE" : 
    {
      "iterator kind" : "AND",
      "input iterators" : [
        {
          "iterator kind" : "ANY_EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "state",
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
                  "variable" : "$u"
                }
              }
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : "CA"
          }
        },
        {
          "iterator kind" : "ANY_LESS_THAN",
          "left operand" :
          {
            "iterator kind" : "CONST",
            "value" : 300
          },
          "right operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "areacode",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$phone"
            }
          }
        },
        {
          "iterator kind" : "ANY_LESS_OR_EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "areacode",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$phone"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 500
          }
        }
      ]
    },
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$u"
          }
        }
      },
      {
        "field name" : "areacode",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "areacode",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$phone"
              }
            }
          ]
        }
      }
    ]
  }
}
}