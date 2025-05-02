compiled-query-plan

{
"query file" : "schemaless/q/upd01.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "UPDATE_ROW",
    "indexes to update" : [ "idx_country_genre", "idx_country_showid", "idx_country_showid_date", "idx_country_showid_seasonnum_minWatched", "idx_showid" ],
    "update clauses" : [
      {
        "iterator kind" : "SET",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "firstName",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$v"
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "CONST",
          "value" : "Jonathan"
        }
      },
      {
        "iterator kind" : "ADD",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "shows",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$v"
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "MAP_CONSTRUCTOR",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "showName"
            },
            {
              "iterator kind" : "CONST",
              "value" : "Casa de papel"
            },
            {
              "iterator kind" : "CONST",
              "value" : "showId"
            },
            {
              "iterator kind" : "CONST",
              "value" : 18
            },
            {
              "iterator kind" : "CONST",
              "value" : "type"
            },
            {
              "iterator kind" : "CONST",
              "value" : "tvseries"
            },
            {
              "iterator kind" : "CONST",
              "value" : "genres"
            },
            {
              "iterator kind" : "ARRAY_CONSTRUCTOR",
              "conditional" : false,
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : "action"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "spanish"
                }
              ]
            }
          ]
        }
      },
      {
        "iterator kind" : "SET",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "numEpisodes",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_SLICE",
            "low bound" : 0,
            "high bound" : 0,
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "seasons",
              "input iterator" :
              {
                "iterator kind" : "ARRAY_SLICE",
                "low bound" : 0,
                "high bound" : 0,
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "shows",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$v"
                  }
                }
              }
            }
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "CONST",
          "value" : 3
        }
      },
      {
        "iterator kind" : "ADD",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "episodes",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_SLICE",
            "low bound" : 0,
            "high bound" : 0,
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "seasons",
              "input iterator" :
              {
                "iterator kind" : "ARRAY_SLICE",
                "low bound" : 0,
                "high bound" : 0,
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "shows",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$v"
                  }
                }
              }
            }
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "MAP_CONSTRUCTOR",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "episodeID"
            },
            {
              "iterator kind" : "CONST",
              "value" : 40
            },
            {
              "iterator kind" : "CONST",
              "value" : "lengthMin"
            },
            {
              "iterator kind" : "CONST",
              "value" : 52
            },
            {
              "iterator kind" : "CONST",
              "value" : "minWatched"
            },
            {
              "iterator kind" : "CONST",
              "value" : 45
            },
            {
              "iterator kind" : "CONST",
              "value" : "date"
            },
            {
              "iterator kind" : "CONST",
              "value" : "2021-05-23"
            }
          ]
        }
      },
      {
        "iterator kind" : "REMOVE",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "ARRAY_SLICE",
          "low bound" : 1,
          "high bound" : 1,
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "seasons",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_SLICE",
              "low bound" : 1,
              "high bound" : 1,
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "shows",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$v"
                }
              }
            }
          }
        }
      }
    ],
    "update TTL" : false,
    "isCompletePrimaryKey" : true,
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Viewers",
        "row variable" : "$$v",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"acct_id":200,"user_id":1},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$v",
      "SELECT expressions" : [
        {
          "field name" : "v",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$v"
          }
        }
      ]
    }
  }
}
}