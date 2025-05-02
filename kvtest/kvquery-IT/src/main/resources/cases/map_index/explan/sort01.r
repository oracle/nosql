compiled-query-plan

{
"query file" : "map_index/q/sort01.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "order by fields at positions" : [ 2, 0 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Boo",
        "row variable" : "$$u",
        "index used" : "idx",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$u",
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
              "variable" : "$$u"
            }
          }
        },
        {
          "field name" : "expenses",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "expenses",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$u"
            }
          }
        },
        {
          "field name" : "sort_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "food",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "expenses",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$u"
              }
            }
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-0",
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
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "expenses",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "expenses",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    }
  ]
}
}