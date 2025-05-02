compiled-query-plan

{
"query file" : "idc_unnest_json/q/map02.q",
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
      }
    ]
  }
}
}