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

setMethod("schema", "Solr", function(x) {
    schema(core(x), query(x))
})

setMethod("ids", "Solr", function(x) {
  uk <- uniqueKey(schema(core(x)))
  if (!is.null(uk))
    x[,uk]
  else NULL
})

setMethod("fieldNames", "Solr", function(x, ...) {
    fieldNames(core(x), query(x), ...)
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
              symbolFactory(x) <- SymbolFactory(SolrFunctionExpression())
              x
          })

undefer <- function(x) {
    symbolFactory(x) <- NULL
    x
}

deferred <- function(x) {
    !is.null(symbolFactory(x))
}

setGeneric("compatible", function(x, y, ...) standardGeneric("compatible"))

setMethod("compatible", c("Solr", "Solr"), function(x, y) {
              params(query(x))$fl <- params(query(y))$fl
              params(query(x))$json$facet <- params(query(y))$json$facet
              identical(query(x), query(y)) && identical(core(x), core(y))
          })

setGeneric("grouping", function(x, ...) standardGeneric("grouping"))
setMethod("grouping", "Solr", function(x) NULL)

setMethod("rename", "Solr", function(x, ...) {
              map <- c(...)
              if (!is.character(map) || any(is.na(map))) {
                  stop("arguments in '...' must be character and not NA")
              }
              badOldNames <- setdiff(map, names(x))
              if (length(badOldNames))
                  stop("Some 'from' names in value not found on 'x': ",
                       paste(badOldNames, collapse = ", "))
              query(x) <- rename(query(x), map)
              x
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

setMethod("searchDocs", c("Solr", "ANY"), function(x, q) {
              query(x) <- searchDocs(query(x), q)
              x
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

mergeGrouping <- function(x, y) {
    if (is.null(x)) return(y)
    if (is.null(y)) return(x)
    lhs <- if (length(y) == 3L) y[[2L]]
    vars <- union(colnames(attr(terms(x), "factors")),
                  colnames(attr(terms(y), "factors")))
    as.formula(paste(lhs, "~", paste(vars, collapse="+")))
}

setMethod("xtabs", "Solr",
          function(formula, data,
                   subset, sparse = FALSE, 
                   na.action, addNA = FALSE, exclude = if (!addNA) NA,
                   drop.unused.levels = FALSE)
              {
                  core <- core(data)
                  formula <- mergeGrouping(grouping(data), formula)
                  data <- query(data)
                  as.table(facets(core, callGeneric())[[formula]])
              })

### TODO
###     - the LHS could support "." to indicate all terms except the RHS,
###     - a named list of functions, all of which are applied, and their name
###       serves as the postfix

setMethod("aggregate", "formula", function(x, ...) {
  aggregateByFormula(x, ...)
})

setGeneric("aggregateByFormula",
           function(formula, data, ...) standardGeneric("aggregateByFormula"),
           signature="data")

setMethod("aggregateByFormula", "ANY", stats:::aggregate.formula)

### NOTE: By default, this drops NAs, just like aggregate.formula,
###       except aggregate.formula drops records with NAs in the
###       response and factors, whereas we only drop those with NAs in
###       the factors. This is consistent with aggregate.data.frame.
setMethod("aggregateByFormula", "Solr",
          function(formula, data, FUN, ..., subset, na.action, simplify = TRUE,
                   count = FALSE) {
              na.action <- normNAAction(na.action)
              useNA <- identical(na.action, na.pass)
              if (!missing(subset)) {
                  query(data) <- rsolr::subset(query(data),
                                               .(substitute(subset)))
              }
              if (!isTRUEorFALSE(simplify)) {
                  stop("'simplify' should be TRUE or FALSE")
              }
              if (!isTRUEorFALSE(count)) {
                  stop("'count' should be TRUE or FALSE")
              }
              functionalStyle <- !missing(FUN)
              if (functionalStyle) {
                  hasLHS <- length(formula) == 3L
                  if (hasLHS) {
                      response <- formula[[2L]]
                      formula <- formula[-2L]
                  }
                  data <- group(as(data, "SolrFrame", strict=FALSE), formula)
                  grouping <- grouping(data)
                  if (hasLHS) {
                      env <- environment(formula)
                      arg <- eval(response, data, env)
                      resultName <- deparse(response)
                  } else {
                      arg <- data
                      resultName <- deparse(substitute(FUN))
                  }
                  result <- FUN(arg, ...)
                  if (is(result, "SolrAggregatePromise")) {
                      query <- facet(query(data), grouping, .(result),
                                     useNA=useNA)
                  } else {
                      ans <- uniqueBy(data, NULL, useNA, count)
                      if (is.data.frame(result)) {
                          ans <- cbind(ans, result)
                      } else {
                          if (simplify) {
                              result <- simplify2array(result)
                          }
                          ans[[resultName]] <- result
                      }
                      return(ans)
                  }
              } else {
                  grouping <- mergeGrouping(grouping(data), formula)
                  query <- facet(query(data), grouping, useNA=useNA, ...)
              }
              fct <- facets(core(data), query)
              if (!is.null(grouping)) {
                  fct <- fct[[grouping]]
              }
              ans <- stats(fct)
              if (!count) {
                  ans$count <- NULL
              }
              ans
          })

setMethod("aggregate", "Solr", function(x, ...) {
              aggregateByFormula(NULL, x, ...)
          })

uniqueBy <- function(x, by, useNA = TRUE, count = FALSE) {
    by <- mergeGrouping(grouping(x), by)
    facet.query <- facet(query(x), by, useNA=useNA)
    ans <- stats(facets(core(x), facet.query)[[by]])
    if (!count) {
        ans$count <- NULL
    }
    ans
}

setMethod("unique", "Solr", function (x, incomparables = FALSE) {
              if (!identical(incomparables, FALSE)) {
                  stop("'incomparables' not supported")
              }
              fields <- resolveFields(core(x), query(x))
              if (!all(indexed(fields))) {
                  stop("cannot compute unique values of unindexed field(s): ",
                       paste0("'", names(fields)[!indexed], "'", collapse=", "))
              }
              grp <- as.formula(paste("~", paste(names(fields), collapse="+")))
              uniqueBy(x, grp)
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
  x[fieldNames]
}

setMethod("as.data.frame", "Solr",
          function(x, row.names = NULL, optional = FALSE, fill = FALSE)
{
  if (!isTRUEorFALSE(optional)) {
    stop("'optional' must be TRUE or FALSE")
  }
  df <- read(core(x), query(x), as="data.frame")
  if (fill) {
    fn <- fieldNames(x, onlyStored=TRUE, includeStatic=TRUE)
    df <- fillMissingFields(df, fn, schema(core(x)))
  }
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
  } else if (!identical(row.names, FALSE)) {
      rownames(df) <- row.names
  }
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
              cat("'", name(core(object)), "' (",
                  "ndoc:", ndoc(object),
                  ", nfield:", nfield(object), ")\n",
                  sep="")
          })
