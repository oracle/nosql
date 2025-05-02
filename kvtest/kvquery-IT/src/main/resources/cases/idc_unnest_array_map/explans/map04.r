compiled-query-plan

{
"query file" : "idc_unnest_array_map/q/map04.q",
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
      "target table" : "User",
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
      "iterator kind" : "VALUES",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "children",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$u"
        }
      }
    },
    "FROM variable" : "$child_info",
    "WHERE" : 
    {
      "iterator kind" : "AND",
      "input iterators" : [
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "age",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$child_info"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 11
          }
        },
        {
          "iterator kind" : "GREATER_THAN",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "school",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "Anna",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "children",
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
            "value" : "sch_1"
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
        "field name" : "friends",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "friends",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$child_info"
          }
        }
      }
    ]
  }
}
}