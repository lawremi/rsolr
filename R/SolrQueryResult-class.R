### =========================================================================
### SolrQueryResult objects
### -------------------------------------------------------------------------
###
### Represents the results returned by evaluating a SolrQuery, with
### the JSON response writer. eval() calls read() on the select URL,
### which uses the Media framework to return a data.frame for CSV, a
### list for JSON, and an XMLDocument for XML. Since JSON is the
### primary response type, and it is complex, we should convert the
### semantically poor list into something more useful. This happens
### through the generic convertSolrQueryResponse(), which is passed
### the raw output, as well as the core and query.
###
### The raw data form a list. While we could convert the list into a
### formal representation of its structure, depending on the Solr
### configuration, there may be elements that do not fit into it. We
### could have a gutter list for those. Or we could just store the
### list itself, or *be* the list, with accessors that retrieve
### specific components.
###

setClass("SolrQueryResult",
         representation(core="SolrCore",
                        query="SolrQuery"),
         contains="VIRTUAL")

setClass("ListSolrQueryResult",
         contains=c("list", "SolrQueryResult"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

ListSolrQueryResult <- function(x, core, query) {
    new("ListSolrQueryResult", x, core=core, query=query)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

docs <- function(x) {
    schema <- augment(schema(core(x)), query(x))
    fromSolr(x$response$docs, schema)
}

setMethod("ndoc", "ListSolrQueryResult",
          function(x) as.integer(x$response$numFound))

summary.SolrQueryResult <- function(object) {
    summary(object)
}

setMethod("summary", "SolrQueryResult", function(object) {
              SolrSummary(object)
          })

setMethod("facets", "SolrQueryResult", function(x) {
              facets(summary(x))
          })

setMethod("ngroup", "ListSolrQueryResult", function(x) {
              vapply(x$grouped, function(g) length(g$groups), integer(1L))
          })

setMethod("groups", c("ListSolrQueryResult", "missing"),
          function(x, by) {
              schema <- schema(core(x))
              lapply(x$grouped, function(grouping) {
                         groups <- grouping$groups
                         docLists <- pluck(groups, "doclist")
                         fromSolrDocList <- function(dl) {
                             fromSolr(dl$docs, schema)
                         }
                         groups <- lapply(docLists, fromSolrDocList)
                         names(groups) <- pluck(groups, "groupValue")
                         groups
                     })
          })

