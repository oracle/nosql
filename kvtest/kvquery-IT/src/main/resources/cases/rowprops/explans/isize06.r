compiled-query-plan

{
"query file" : "rowprops/q/isize06.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-1",
  "input iterator" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "input iterator" :
    {
      "iterator kind" : "GROUP",
      "input variable" : "$gb-0",
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "Foo",
          "row variable" : "$f",
          "index used" : "idx_city_phones",
          "covering index" : true,
          "index row variable" : "$f_idx",
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$f_idx",
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
                "variable" : "$f_idx"
              }
            }
          },
          {
            "field name" : "isize",
            "field expression" : 
            {
              "iterator kind" : "FUNC_MKINDEX_STORAGE_SIZE",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f_idx"
              }
            }
          }
        ]
      },
      "grouping expressions" : [
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-0"
          }
        }
      ],
      "aggregate functions" : [
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "isize",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-0"
            }
          }
        }
      ]
    }
  },
  "grouping expressions" : [
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "id",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-1"
      }
    }
  ],
  "aggregate functions" : [
    {
      "iterator kind" : "FUNC_SUM",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "isize",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-1"
        }
      }
    }
  ]
}
}