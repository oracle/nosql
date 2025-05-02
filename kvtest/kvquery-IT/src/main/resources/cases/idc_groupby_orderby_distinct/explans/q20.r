compiled-query-plan

{
"query file" : "idc_groupby_orderby_distinct/q/q20.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
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
            "target table" : "SimpleDatatype",
            "row variable" : "$$SimpleDataType",
            "index used" : "primary index",
            "covering index" : true,
            "index scans" : [
              {
                "equality conditions" : {},
                "range conditions" : {}
              }
            ],
            "position in join" : 0
          },
          "FROM variable" : "$$SimpleDataType",
          "SELECT expressions" : [
            {
              "field name" : "gb-0",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "id",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$SimpleDataType"
                }
              }
            },
            {
              "field name" : "gb-1",
              "field expression" : 
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "shardId",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$SimpleDataType"
                }
              }
            }
          ]
        },
        "grouping expressions" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "gb-0",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$gb-1"
            }
          },
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "gb-1",
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
        "field name" : "gb-0",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-2"
        }
      },
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "gb-1",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-2"
        }
      }
    ],
    "aggregate functions" : [

    ]
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "shardId",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "gb-1",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "id",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "gb-0",
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