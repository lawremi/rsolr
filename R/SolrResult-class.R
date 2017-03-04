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

setClass("Grouping",
         representation(groups="list",
                        schema="SolrSchema"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

ListSolrResult <- function(x, core, query) {
    new("ListSolrResult", x, core=core, query=query)
}

Grouping <- function(groups, schema) {
    groups <- groups$groups
    names(groups) <- pluck(groups, "groupValue")
    groups <- pluck(pluck(groups, "doclist"), "docs")
    new("Grouping", groups=groups, schema=schema)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setMethod("docs", "ListSolrResult", function(x) {
              if (grouped(query(x))) {
                  groupings(x)[[1L]]
              } else {
                  fromSolr(x$response$docs, schema(core(x)), query(x))
              }
          })

setMethod("ndoc", "ListSolrResult",
          function(x) as.integer(x$response$numFound))

setMethod("facets", "ListSolrResult", function(x) {
              Facets(as.list(x$facets), as.list(json(query(x))$facet),
                     schema(core(x)))
          })

setMethod("ngroup", "ListSolrResult", function(x) {
              vapply(x$grouped, function(g) length(g$groups), integer(1L))
          })

setMethod("groupings", "ListSolrResult", function(x) {
              schema <- augment(schema(core(x)), query(x))
              lapply(x$grouped, Grouping, schema)
          })

setMethod("ngroup", "Grouping", function(x) {
              length(x@groups)
          })

setMethod("schema", "Grouping", function(x) x@schema)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setAs("Grouping", "list", function(from) {
          lapply(from@groups, fromSolr, schema(from))
      })

relist1 <- function(x, skeleton) { # non-recursive variant of relist
    to <- cumsum(lengths(skeleton, use.names=FALSE))
    from <- c(1L, head(to, -1L) + 1L)
    mapply(function(from, to) x[from:to], from, to, SIMPLIFY=FALSE)
}

setAs("Grouping", "data.frame", function(from) {
          docs <- as(unlist(from@groups, recursive=FALSE), "DocList")
          df <- fromSolr(as.data.frame(docs), schema(from))
          df[] <- lapply(df, relist1, from@groups)
          df
      })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "Grouping", function(object) {
              cat("Grouping object\n")
              cat("ngroup: ", ngroup(object), "\n")
          })
