### =========================================================================
### Solr objects
### -------------------------------------------------------------------------
### 
### Combines a SolrCore (endpoint) with a persistent SolrQuery
###
### The intent is to get the benefit of the lazy SolrQuery without
### having to juggle two separate objects.
###
### Evaluation is forced by:
### - summary operations (summary, aggregate, xtabs) and
### - coercion (as.list, as.data.frame, [,drop=TRUE]).
### Only endomorphic operations are lazy:
### - subset, [,drop=FALSE]
### - transform
### - sort

### 
### TODO: add with(), within(), eval(), as(,"environment"), via active [[
###

setClass("Solr",
         representation(core="SolrCore",
                        query="SolrQuery"),
         contains="VIRTUAL")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

core <- function(x) x@core

query <- function(x) x@query
`query<-` <- function(x, value) {
  x@query <- value
  x
}

setMethod("ids", "Solr", function(x) {
  uk <- uniqueKey(schema(core(x)))
  if (!is.null(uk))
    x[,uk]
  else NULL
})

normFL <- function(fl) {
  if (is.null(names(fl)))
    fl
  else {
    fl[names(fl) != ""] <- names(fl)[names(fl) != ""]
    unname(fl)
  }
}

setMethod("fieldNames", "Solr", function(x, ...)
{
  fl <- normFL(params(query(x))$fl)
  glob <- grepl("*", fl, fixed=TRUE)
  if (any(glob)) {
    fieldNames(core(x), patterns=fl, ...)
  } else {
    fl
  }
})

setMethod("nfield", "Solr", function(x) {
  length(fieldNames(x))
})

setMethod("ndoc", "Solr", function(x) {
  ndoc(core(x), query(x))
})

setMethod("staticFieldNames", "Solr", function(x) {
  staticFieldNames(schema(core(x)))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### CREATE/UPDATE/DELETE
###

setReplaceMethod("$", "Solr", function(x, name, value) {
  x[[name]] <- value
  x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### READ
###

setMethod("$", "Solr", function(x, name) {
              x[[name]]
          })

setMethod("subset", "Solr", function(x, ...) {
  query(x) <- subset(query(x), ..., select.from=fieldNames(x, onlyStored=TRUE))
  x
})

window.Solr <- function(x, ...) window(x, ...)

setMethod("window", "Solr", function (x, ...) {
              query(x) <- window(query(x), ...)
              x
          })

head.Solr <- function (x, n = 6L, ...) {
  query(x) <- head(query(x), n, ...)
  x
}
setMethod("head", "Solr", head.Solr)

tail.Solr <- function (x, n = 6L, ...) {
  query(x) <- tail(query(x), n, ...)
  x
}
setMethod("tail", "Solr", tail.Solr)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Transforming
###

transform.Solr <- function(`_data`, ...) transform(`_data`, ...)

setMethod("transform", "Solr", function (`_data`, ...) {
  query(`_data`) <- transform(query(`_data`), ...)
  `_data`
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Sorting
###

setMethod("sort", "Solr", function (x, decreasing = FALSE, ...) {
  query(x) <- sort(query(x), decreasing=decreasing, ...)
  x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Summarizing
###
##

summary.Solr <- function(object, ...)
{
  summary(core(object),
          query = summary(query(object), schema(core(object)), ...))
}

setMethod("summary", "Solr", summary.Solr)

setMethod("xtabs", "Solr",
          function(formula, data,
                   subset, sparse = FALSE, 
                   na.action, exclude = NA,
                   drop.unused.levels = FALSE)
          {
            if (!all(names(match.call())[-1L] %in%
                     c("formula", "data", "exclude"))) {
              stop("all args but 'formula', 'data' and 'exclude' are ignored")
            }
            q <- facet(query(data), formula, useNA=is.null(exclude))
            facet(core(data), q)[[1L]]
          })

setMethod("aggregate", "formula", function(x, ...) {
  aggregateByFormula(x, ...)
})

setGeneric("aggregateByFormula",
           function(formula, data, ...) standardGeneric("aggregateByFormula"),
           signature="data")

setMethod("aggregateByFormula", "ANY", stats:::aggregate.formula)

setMethod("aggregateByFormula", "Solr", function(formula, data, FUN, ...) {
  FUN(SolrAggregation(formula, data), ...)
})

setMethod("aggregate", "Solr", function(x, ...) {
              stats(facets(fulfill(facet(query(x), ...))))
          })

uniqueBy <- function(x, by) {
    ans <- subset(as.data.frame(xtabs(by, x)), Freq > 0L, select=-Freq)
    ans <- ans[order(ans[[1L]]),,drop=FALSE]
    rownames(ans) <- NULL
    as(ans, "DocCollection")
}

setMethod("unique", "Solr", function (x, incomparables = FALSE) {
              if (!identical(incomparables, FALSE)) {
                  stop("'incomparables' not supported")
              }
              f <- as.formula(paste("~",
                                    paste(fieldNames(x, onlyIndexed=TRUE),
                                          collapse="+")))
              uniqueBy(x, f)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

as.data.frame.Solr <- function(x, row.names = NULL, optional = FALSE, ...)
    as.data.frame(x, row.names=row.names, optional=optional, ...)

fillMissingFields <- function(x, fieldNames, schema) {
  fields <- fields(schema, setdiff(fieldNames, names(x)))
  modes <- fieldTypes(schema)[typeName(fields)]
  x[names(fields)] <- lapply(modes, fromSolr,
                             x=rep(NA, nrow(as.data.frame(x))))
  x
}

setMethod("as.data.frame", "Solr",
          function(x, row.names = NULL, optional = FALSE, fill = FALSE)
{
  if (!isTRUEorFALSE(optional)) {
    stop("'optional' must be TRUE or FALSE")
  }
  df <- read(core(x), query(x), as="data.frame")
  fn <- fieldNames(x, onlyStored=TRUE, includeStatic=fill)
  if (fill) {
    df <- fillMissingFields(df, fn, schema(core(x)))
  }
  df <- df[,intersect(fn, colnames(df)),drop=FALSE]
  if (!optional) {
    colnames(df) <- make.names(colnames(df), unique = TRUE)
  }
  uk <- uniqueKey(schema(core(x)))
  if (isTRUE(row.names)) {
    if (is.null(uk)) {
      stop("automatic rownames requested, but schema lacks a 'uniqueKey'")
    }
    row.names <- df[[uk]]
    if (is.null(row.names)) {
      row.names <- ids(x)
    }
  }
  rownames(df) <- row.names
  df
})

as.data.frame.Solr <- function(x, row.names = NULL, optional = FALSE,
                               fill = FALSE)
{
  as.data.frame(x, row.names=row.names, optional=optional, fill=fill)
}

setAs("Solr", "data.frame", function(from) {
          as.data.frame(from, optional=TRUE)
      })

as.list.Solr <- function(x) {
  read(core(x), query(x))
}

setMethod("as.list", "Solr", as.list.Solr)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "Solr", function(object) {
  cat(class(object), " (", ndoc(object), "x", nfield(object), ")\n", sep="")
  cat("core: '", name(core(object)), "'\n", sep="")
  query <- as.character(query(object))
  defaults <- as.character(SolrQuery())
  drop <- which(defaults[names(query)] == query)
  if (length(drop) > 0L) {
    query <- query[names(query) %in% names(query)[-drop]]
  }
  labeled.query <- if (length(query) > 0L) {
    paste0(names(query), "='", query, "'")
  } else character()
  cat(BiocGenerics:::labeledLine("query", labeled.query))
})
