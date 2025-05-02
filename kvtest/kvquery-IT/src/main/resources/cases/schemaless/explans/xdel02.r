compiled-query-plan

{
"query file" : "schemaless/q/xdel02.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "DELETE_ROW",
      "positions of primary key columns in input row" : [ 1, 2, 3 ],
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "jsoncol",
          "row variable" : "$j",
          "index used" : "primary index",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {"majorKey1":"cc"},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$j",
        "SELECT expressions" : [
          {
            "field name" : "j",
            "field expression" : 
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$j"
            }
          },
          {
            "field name" : "majorKey1_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "majorKey1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$j"
              }
            }
          },
          {
            "field name" : "majorKey2_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "majorKey2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$j"
              }
            }
          },
          {
            "field name" : "minorKey_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "minorKey",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$j"
              }
            }
          }
        ]
      }
    },
    "FROM variable" : "$j",
    "SELECT expressions" : [
      {
        "field name" : "j",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "j",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$j"
          }
        }
      }
    ]
  }
}
}