compiled-query-plan

{
"query file" : "delete/q/rdel07.q",
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
      "positions of primary key columns in input row" : [ 1 ],
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "Foo",
          "row variable" : "$$f",
          "index used" : "idx_areacode",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {"info.address.phones[].areacode":0},
              "range conditions" : {}
            }
          ],
          "key bind expressions" : [
            {
              "iterator kind" : "EXTERNAL_VAR_REF",
              "variable" : "$areacode"
            }
          ],
          "map of key bind expressions" : [
            [ 0 ]
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$f",
        "SELECT expressions" : [
          {
            "field name" : "phones",
            "field expression" : 
            {
              "iterator kind" : "ARRAY_CONSTRUCTOR",
              "conditional" : true,
              "input iterators" : [
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "phones",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "address",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "info",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$f"
                      }
                    }
                  }
                }
              ]
            }
          },
          {
            "field name" : "id_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "id",
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
        "field name" : "phones",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "phones",
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