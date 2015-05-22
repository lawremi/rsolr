### =========================================================================
### SolrResult objects
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

setClass("SolrResult",
         representation(core="SolrCore",
                        query="SolrQuery"),
         contains="VIRTUAL")

setClass("ListSolrResult",
         contains=c("list", "SolrResult"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

ListSolrResult <- function(x, core, query) {
    new("ListSolrResult", x, core=core, query=query)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

docs <- function(x) {
    schema <- augment(schema(core(x)), query(x))
    fromSolr(x$response$docs, schema)
}

setMethod("ndoc", "ListSolrResult",
          function(x) as.integer(x$response$numFound))

setMethod("facets", "ListSolrResult", function(x) {
              Facets(x$facets, params(query(x))$json.facet)
          })

setMethod("ngroup", "ListSolrResult", function(x) {
              vapply(x$grouped, function(g) length(g$groups), integer(1L))
          })

groupsAsList <- function(x, schema) {
    lapply(x, fromSolr, schema)
}

groupsAsDataFrame <- function(x, schema, ...) {
    docs <- as(unlist(x, recursive=FALSE), "DocList")
    df <- fromSolr(as.data.frame(docs, ...), schema)
    as.data.frame(lapply(df, relist, x))
}

setMethod("groupings", c("ListSolrResult", "missing"),
          function(x, by, as=c("list", "data.frame"), ...) {
              schema <- schema(core(x))
              as <- match.arg(as)
              lapply(x$grouped, function(grouping) {
                         groups <- grouping$groups
                         names(groups) <- pluck(groups, "groupValue")
                         docs <- pluck(pluck(groups, "doclist"), "docs")
                         if (as == "list") {
                             groupsAsList(docs, schema, ...)
                         } else {
                             groupsAsDataFrame(docs, schema, ...)
                         }
                     })
          })

AsDocs <- function(type) {
    function(from) {
        if (grouped(query(from))) {
            groupings(from, as=type)[[1L]]
        } else {
            as(docs(from), type)
        }
    }
}

setAs("SolrResult", "list", AsDocs("list"))
setAs("SolrResult", "data.frame", AsDocs("data.frame"))
