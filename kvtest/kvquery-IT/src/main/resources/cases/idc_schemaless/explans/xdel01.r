compiled-query-plan

{
"query file" : "idc_schemaless/q/xdel01.q",
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
          "row variable" : "$$jc",
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
        "FROM variable" : "$$jc",
        "WHERE" : 
        {
          "iterator kind" : "FUNC_REGEX_LIKE",
          {
            "iterator kind" : "CAST",
            "target type" : "String",
            "quantifier" : "*",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "value",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "menu",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "menu",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$jc"
                  }
                }
              }
            }
          }
          {
            "iterator kind" : "CONST",
            "value" : ".*e3.*"
          }
        },
        "SELECT expressions" : [
          {
            "field name" : "jc",
            "field expression" : 
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$jc"
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
                "variable" : "$$jc"
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
                "variable" : "$$jc"
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
                "variable" : "$$jc"
              }
            }
          }
        ]
      }
    },
    "FROM variable" : "$$jc",
    "SELECT expressions" : [
      {
        "field name" : "jc",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "jc",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$jc"
          }
        }
      }
    ]
  }
}
}