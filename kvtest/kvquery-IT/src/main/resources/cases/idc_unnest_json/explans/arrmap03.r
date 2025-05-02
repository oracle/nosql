compiled-query-plan

{
"query file" : "idc_unnest_json/q/arrmap03.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "User_json",
      "row variable" : "$u",
      "index used" : "idx_phones2",
      "covering index" : false,
      "index row variable" : "$u_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "LESS_THAN",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info.addresses[].phones[][].kind",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$u_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : "w"
        }
      },
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
    "FROM variable" : "$child",
    "WHERE" : 
    {
      "iterator kind" : "AND",
      "input iterators" : [
        {
          "iterator kind" : "GREATER_THAN",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "school",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$child"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : "sch_1"
          }
        },
        {
          "iterator kind" : "LESS_THAN",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "school",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$child"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : "sch_3"
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
          "field name" : "#id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$u_idx"
          }
        }
      },
      {
        "field name" : "areacode",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info.addresses[].phones[][].areacode",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$u_idx"
          }
        }
      }
    ]
  }
}
}