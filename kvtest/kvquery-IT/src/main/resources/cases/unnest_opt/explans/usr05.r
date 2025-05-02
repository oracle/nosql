compiled-query-plan

{
"query file" : "unnest_opt/q/usr05.q",
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
      "iterator kind" : "ARRAY_FILTER",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "friends",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "anna",
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
      }
    },
    "FROM variable" : "$friend",
    "WHERE" : 
    {
      "iterator kind" : "EQUAL",
      "left operand" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$friend"
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : "mark"
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