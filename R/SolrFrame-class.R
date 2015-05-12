### =========================================================================
### SolrFrame objects
### -------------------------------------------------------------------------
###
### High-level object that represents a Solr core as a data frame
###

setClass("SolrFrame", contains=c("Solr", "Context"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

.SolrFrame <- function(core, query=SolrQuery()) {
  new("SolrFrame", core=core, query=query)
}

SolrFrame <- function(uri, ...) {
  .SolrFrame(SolrCore(uri, ...))
}

setMethod("dimnames", "SolrFrame", function(x) {
  list(rownames(x), colnames(x))
})

setMethod("colnames", "SolrFrame", function(x) {
  fieldNames(x)
})

setMethod("rownames", "SolrFrame", function(x) {
  ids(x)
})

setMethod("dim", "SolrFrame", function(x) {
  c(nrow(x), ncol(x))
})

setMethod("nrow", "SolrFrame", function(x) {
  ndoc(x)
})

setMethod("NROW", "SolrFrame", function(x) {
  nrow(x)
})

setMethod("ncol", "SolrFrame", function(x) {
  nfield(x)
})

setMethod("NCOL", "SolrFrame", function(x) {
  ncol(x)
})

setMethod("length", "SolrFrame", function(x) {
  nfield(x)
})

setMethod("names", "SolrFrame", function(x) {
  fieldNames(x)
})

setMethod("fieldNames", "SolrFrame", function(x, includeStatic=TRUE, ...) {
  callNextMethod(x, includeStatic=includeStatic, ...)
})

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

setReplaceMethod("[", "SolrFrame", function(x, i, j, ..., value) {
  oneD <- (nargs() - length(list(...))) == 3L
  if (oneD) {
    if (missing(i))
      x[,,...] <- value
    else
      x[,i,...] <- value
    return(x)
  }
  x <- as(x, "SolrList")
  as(callGeneric(), "SolrFrame")
})

setReplaceMethod("[[", "SolrFrame", function(x, i, j, ..., value) {
  if (!missing(j))
    warning("argument 'j' is ignored")
  if (missing(i))
    stop("argument 'i' cannot be missing")
  x[,i] <- value
  x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### READ
###

setMethod("[[", "SolrFrame", function(x, i, j, ...) {
  if (missing(i))
    stop("'i' cannot be missing")
  if (!isSingleString(i))
    stop("'i' must be a single, non-NA string")
  if (!missing(j))
    warning("argument 'j' is ignored")
  x[,i,...]
})

setMethod("[", "SolrFrame", function(x, i, j, ..., drop = TRUE) {
  if (!isTRUEorFALSE(drop)) {
    stop("'drop' must be TRUE or FALSE")
  }
  oneD <- (nargs() - !missing(drop)) == 2L
  if (oneD) {
    if (!missing(drop) && drop) {
      stop("'drop' must be FALSE for list-style extraction")
    }
    return(x[,i,drop=FALSE,...])
  }
  x <- as(x, "SolrList")
  ans <- callGeneric()
  if (is(ans, "Solr")) {
    ans <- as(ans, "SolrFrame")
  } else if (is.list(ans)) {
    fieldNames <- fieldNames(x, onlyStored=TRUE, includeStatic=TRUE)
    ans <- fillMissingFields(ans, fieldNames, schema(core(x)))[fieldNames]
  }
  ans
})

setMethod("eval", c("SolrExpression", "SolrFrame"),
          function (expr, envir, enclos) {
              undefer(transform(envir, x = .(expr)))$x
          })

setMethod("eval", c("SolrAggregateExpression", "SolrFrame"),
          function (expr, envir, enclos) {
              aggregate(envir, x = .(expr))$x
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setAs("Solr", "SolrFrame", function(from) {
  .SolrFrame(core(from), query(from))
})

as.data.frame.SolrFrame <- function(x, row.names = NULL, optional = FALSE,
                                    fill = TRUE)
{
  as.data.frame(x, row.names=row.names, optional=optional, fill=fill)
}

setMethod("as.data.frame", "SolrFrame",
          function(x, row.names = NULL, optional = FALSE, fill = TRUE) {
            callNextMethod(x, row.names=row.names, optional=optional, fill=fill)
          })

as.list.SolrFrame <- function(x, ...) {
    as.list(x, ...)
}

setMethod("as.list", "SolrFrame", function(x, lazy=FALSE, ...) {
              lapply(fieldNames(x), `[[`, x=x, lazy=lazy, ...)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Summarizing
###

summary.SolrFrame <- function(object, ...) {
    summary(object, ...)
}

setMethod("summary", "SolrFrame", function(object) {
              types <- fieldTypes(schema(core(object)), fieldNames(object))
              num <- vapply(types, is, logical(1L), "NumericField")
              p <- c(0.25, 0.5, 0.75)
              query <- query(object)
              for (f in fieldNames(object)[num])
                  query <- facet(query, NULL, min(.field(f)),
                                 mean(.field(f)), quantile(.field(f), p),
                                 max(.field(f)))
              ## stats <- lapply(as.list(x[num], lazy=TRUE), function(xi) {
              ##                     list(min=min(xi), mean=mean(xi),
              ##                          quantile=quantile(xi, p), max=max(xi))
              ##                 })
              ## stats <- lapply(unlist(stats), expr)
              ## query <- facet(facet(query(object), .stats=stats), of[!num])
              query <- facet(query, of[!num])
              f <- facets(solr(x), query)
### TODO: construct a summary object here that prints nicely, see SolrSummary
          })


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrFrame", function(object) {
  callNextMethod()
  out <- makePrettyMatrixForCompactPrinting(object)
  print(out, quote=FALSE, right=TRUE, max=length(out))
})
