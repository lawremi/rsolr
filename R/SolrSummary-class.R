### =========================================================================
### SolrSummary objects
### -------------------------------------------------------------------------
###
### Solr can summarize as tables or sets of statistics. Below, we
### define the data structures for representing Solr summaries.
###

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Classes
###

### Can represent stats on a single field, or conditional every level
### of a field.  A query should generate an array of these objects,
### variable X factor. Could be a 3D array, except there is a variable
### number of levels per column.
setClass("SolrStats",
         representation(min="numeric",
                        max="numeric",
                        sum="numeric",
                        count="numeric", # Solr 'long'
                        missing="numeric", # Solr 'long'
                        sumOfSquares="numeric",
                        mean="numeric",
                        stddev="numeric"))

setClass("SolrStatsList", contains="list",
         validity=function(object) {
           validHomogeneousList(object, "SolrStats")
         })

setClass("SolrStatsFacet",
         representation(levels="character"),
         contains="SolrStats")

setClass("SolrStatsField",
         representation(facets="SolrStatsList"),
         contains="SolrStats")

setClass("TableList", contains="list",
         validity=function(object) {
           validHomogeneousList(object, "table")
         })

### FIXME: There is an issue here with heterogeneity. Keeping the
### stats and tables in separate components benefits clarity, but
### there is still complexity. Question: should there be separate
### methods on SolrCore, or do we want to compute complex summaries in
### a single call?  SolrQuery supports it, and there is at least one
### use case (a global summary).

### Compare:
### stats(summary(sc, q))$field
### stats(sc, q)$field
### where stats() returns a SolrStatsArray.

### In theory, both could be supported. Or summary could just return a
### result for display, like summary,data.frame. We should probably
### keep summary returning actual values, because it is probably
### common for the user to want to request both tables and stats
### simultaneously.

setClass("SolrSummary",
         representation(stats="SolrStatsList",
                        facets="TableList"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

SolrSummary <- function(x) {
  new("SolrSummary", facets=parseFacetTableList(x),
      stats=parseSolrStatsList(x))
}

SolrStats <- function(x) {
  x$facets <- parseSolrStatsFacets(x$facets)
  do.call(new, c("SolrStats", x))
}

SolrStatsFacet <- function(x) {
  do.call(new, c("SolrStatsFacet", levels=names(x), do.call(rbind, x)))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setGeneric("facets", function(x, ...) standardGeneric("facets"))
setMethod("facets", "ANY", function(x) x@facets)

setGeneric("stats", function(x, ...) standardGeneric("stats"))
setMethod("stats", "ANY", function(x) x@stats)

setMethod("length", "SolrStats", function(x) {
  length(x@min)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Parsing
###

parseFacetTableList <- function(x) {
  new("TableList", c(parseFacetFieldTables(x), parseFacetQueryTables(x),
                     parseFacetPivotTables(x)))
}

parseFacetQueryTables <- function(x) {
  lapply(x$facet_counts$facet_queries, function(xi) {
    mode(xi) <- "integer"
    as.table(c("FALSE"=x$response$numFound-xi, "TRUE"=xi))
  })
}

parseFacetFieldTables <- function(x) {
  lapply(x$facet_counts$facet_fields, function(xi) {
    mode(xi) <- "integer"
    as.table(xi)
  })
}

## depends on facet.missing=true
parsePivot <- function(pivot, fields) {
  if (length(fields) == 1L) {
    df <- raggedListToDF(pivot)
    df$field <- NULL
    df$value <- as.factor(df$value)
  } else {
    df <- do.call(rbind, lapply(pivot$pivot, parsePivot, fields=fields[-1L]))
    value <- if (is.null(pivot$value)) NA else pivot$value
    df <- cbind(as.factor(value), df)
  }
  colnames(df)[1L] <- fields[1L]
  df
}

parseFacetPivotTables <- function(x) {
  fields <- strsplit(x$facet_counts$facet_pivot, ",", fixed=TRUE)
  mapply(function(pivot, fields) {
    dfToTable(parsePivot(pivot, fields))
  }, x$facet_pivot, fields, SIMPLIFY=FALSE)
}

parseSolrStatsList <- function(x) {
  lapply(x$stats_fields, SolrStats)
}

parseSolrStatsFacets <- function(x) {
  new("SolrStatsList", lapply(x, SolrStatsFacet))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

as.data.frame.SolrStats <- function(x, row.names = NULL, optional = FALSE) {
  slots <- lapply(slotNames(x), slot, object=x)
  slots$facets <- NULL
  data.frame(slots, row.names=row.names, check.names=!optional)
}

setMethod("as.data.frame", "SolrStats", as.data.frame.SolrStats)

as.data.frame.SolrStatsList <- function(x, row.names = NULL, optional = FALSE) {
  cbind(field=rep(names(x), elementLengths(x)),
        do.call(rbind, lapply(x, as.data.frame, row.names=row.names,
                              optional=optional)))
}

setMethod("as.data.frame", "SolrStatsList", as.data.frame.SolrStatsList)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrStats", function(object) {
  show(as.data.frame(object))
})

setMethod("show", "SolrStatsList", function(object) {
  show(as.data.frame(object))
})

setMethod("show", "SolrStatsField", function(object) {
  callNextMethod()
  cat(labeledLine("facets", names(facets(object))), "\n")
})

setMethod("show", "TableList", function(object) {
  show(do.call(cbind, lapply(object, formatTable, nlevels=6L)))
})

setMethod("show", "SolrSummary", function(object) {
  cat("facets:\n")
  show(facets(object))
  cat("stats:\n")
  show(stats(object))
})
