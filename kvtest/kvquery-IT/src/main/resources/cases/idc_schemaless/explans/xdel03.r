compiled-query-plan

{
"query file" : "idc_schemaless/q/xdel03.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
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
          "row variable" : "$$f",
          "index used" : "idx_index",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {"index":0},
              "range conditions" : {}
            }
          ],
          "key bind expressions" : [
            {
              "iterator kind" : "CONST",
              "value" : 900
            }
          ],
          "map of key bind expressions" : [
            [ 0 ]
          ],
          "bind info for in3 operator" : [
            {
              "theNumComps" : 1,
              "thePushedComps" : [ 0 ],
              "theIndexFieldPositions" : [ 0 ]
             }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$f",
        "SELECT expressions" : [
          {
            "field name" : "f",
            "field expression" : 
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
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
                "variable" : "$$f"
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
                "variable" : "$$f"
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
                "variable" : "$$f"
              }
            }
          }
        ]
      }
    },
    "FROM variable" : "$$f",
    "SELECT expressions" : [
      {
        "field name" : "f",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "f",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f"
          }
        }
      }
    ]
  }
}
}