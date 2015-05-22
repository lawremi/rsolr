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
### - summary operations (summary, aggregate, xtabs, head/tail) and
### - coercion (as.list, as.data.frame, [,drop=TRUE]).
### Only endomorphic operations are lazy:
### - subset, [,drop=FALSE]
### - transform
### - sort

### Thoughts on generalizing the Solr* data structures:
###
### We could introduce abstract classes that implement much of the
### functionality of the Solr* classes. These can then be reused to
### implement other database backends.
###
### Solr      =>DbSet
### SolrList  =>DbList
### SolrFrame =>DbFrame
### SolrQuery =>DbQuery
### SolrCore  =>Database
###
### Call the package "rdb".

setClassUnion("SymbolFactoryORNULL", c("SymbolFactory", "NULL"))

setClass("Solr",
         representation(core="SolrCore",
                        query="SolrQuery",
                        symbolFactory="SymbolFactoryORNULL"),
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

setMethod("symbolFactory", "Solr", function(x) x@symbolFactory)

setReplaceMethod("symbolFactory", "Solr", function(x, value) {
                     x@symbolFactory <- value
                     x
                 })

setGeneric("defer", function(x, ...) standardGeneric("defer"))

setMethod("defer", "Solr", function(x) {
              symbolFactory(x) <- symbolFactory(SolrFunctionExpression())
              x
          })

undefer <- function(x) {
    symbolFactory(x) <- NULL
    x
}

setGeneric("compatible", function(x, y, ...) standardGeneric("compatible"))

setMethod("compatible", c("Solr", "Solr"), function(x, y) {
              identical(query(x), query(y)) && identical(core(x), core(y))
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
  query(x) <- subset(query(x), ..., select.from=fieldNames(x))
  x
})

## It would be more consistent with R if we carried over fields in the
## frame to the new environment. For example, get("foo") would work
## for a field 'foo', even when 'foo' is not in the
## expression. However, there are contexts that are unable to
## enumerate their fields, as the set is practically infinite. This is
## generally true of NoSQL databases. Thus, we instead extract the
## variables from the expression.

setMethod("eval", c("language", "Solr"), function (expr, envir, enclos) {
              expr <- preprocessExpression(expr, enclos)
              eval(expr, varsEnv(expr, envir, enclos))
          })

setMethod("with", "Solr", function (data, expr, ...) {
              eval(substitute(expr), data, parent.frame(), ...)
          })

setMethod("within", "Solr", function (data, expr, ...) {
              dataEnv <- list2LazyEnv(data, top_prenv(expr))
              e <- new.env(parent=dataEnv)
              eval(substitute(expr), e, top_prenv(expr))
              data[names(e)] <- as.list(e, all.names=TRUE)
              data
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Transforming
###

### TODO: there should be a `_list` argument that is taken unquoted, to allow:
###  transform(x, lapply(subset(x, select=foo:bar), abs))

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

### TODO
###     - the LHS could support "." to indicate all terms not on RHS,
###     - a named list of functions, all of which are applied, and their name
###       serves as the postfix,
###     - named, quoted arguments adding stats as columns,

### How does aggregation work?
###
### It could split() itself by the formula, which yields a SplitSolr,
### or a SplitSolrPromise if there is an LHS. When given a function,
### we always have a Promise, and that Promise is passed to the
### function. If we have expressions, those are passed directly (via
### ...) to facet,SolrQuery. The query is evaluated, and the facet
### result corresponding to the formula is extracted.

### A serious issue is that the computation is taking place on the
### entire dataset, even though it is conceptually happening
### group-wise.  For example, length() on a SplitSolrPromise returns
### the number of groups. This makes sense, but causes problems when
### we try to fake looping in aggregate(). ndoc() will return the
### equivalent of lengths(), which will work, but the strange behavior
### of length() will confuse the user. We'll see how bad it is.

setMethod("aggregate", "formula", function(x, ...) {
  aggregateByFormula(x, ...)
})

setGeneric("aggregateByFormula",
           function(formula, data, ...) standardGeneric("aggregateByFormula"),
           signature="data")

setMethod("aggregateByFormula", "ANY", stats:::aggregate.formula)

setMethod("aggregateByFormula", "Solr", function(formula, data, FUN, ...) {
              if (!missing(FUN)) {
                  query <- facet(query(x), FUN(group(data, formula), ...))
              } else {
                  query <- facet(query(x), ...)
              }
              stats(facets(core(x), query)[[formula]])
})

setMethod("aggregate", "Solr", function(x, ...) {
              aggregateByFormula(NULL, x, ...)
          })

uniqueBy <- function(x, by) {
    ans <- subset(as.data.frame(xtabs(by, x, exclude=NULL)),
                  Freq > 0L, select=-Freq)
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

window.Solr <- function(x, ...) window(x, ...)

setMethod("window", "Solr", function (x, ...) {
              query(x) <- window(query(x), ...)
              as(x, "DocCollection")
          })

head.Solr <- function(x, n = 6L, ...) head(x, n=n, ...)

setMethod("head", "Solr", function (x, n = 6L, ...) {
              query(x) <- head(query(x), n, ...)
              as(x, "DocCollection")
          })

tail.Solr <- function(x, n = 6L, ...) tail(x, n=n, ...)

setMethod("tail", "Solr", function (x, n = 6L, ...) {
              query(x) <- tail(query(x), n, ...)
              as(x, "DocCollection")
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

as.data.frame.Solr <- function(x, row.names = NULL, optional = FALSE, ...)
    as.data.frame(x, row.names=row.names, optional=optional)

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
  if (isTRUE(row.names)) {
    uk <- uniqueKey(schema(core(x)))      
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

setAs("Solr", "data.frame", function(from) {
          as.data.frame(from, optional=TRUE)
      })

as.list.Solr <- function(x, ...) {
    as.list(x, ...)
}

setMethod("as.list", "Solr", function(x) {
              read(core(x), query(x))
          })

setAs("Solr", "environment", function(from) list2LazyEnv(from))

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
