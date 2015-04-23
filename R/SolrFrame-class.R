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

SolrList <- function(x) {
    newSolr("SolrFrame", x)
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
  x[,i]
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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setAs("Solr", "SolrFrame", function(from) {
          SolrFrame(query(from))
      })

setAs("SolrQuery", "SolrFrame", function(from) {
          SolrFrame(from)
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

setMethod("eval", c("SolrExpression", "SolrFrame"),
          function (expr, envir, enclos) {
              transform(envir, x = .(expr))$x
          })

setMethod("eval", c("SolrAggregateExpression", "SolrFrame"),
          function (expr, envir, enclos) {
              aggregate(envir, x = .(expr))$x
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrFrame", function(object) {
  callNextMethod()
  out <- makePrettyMatrixForCompactPrinting(object)
  print(out, quote=FALSE, right=TRUE, max=length(out))
})
