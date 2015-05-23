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

.SolrFrame <- function(core, query=SolrQuery(), symbolFactory=NULL) {
  new("SolrFrame", core=core, query=query, symbolFactory=symbolFactory)
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
              ans <- ids(x)
              if (is.null(ans)) {
                  ans <- as.character(seq_len(nrow(x)))
              }
              ans
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
  .Solr_2DBracket(x, i, j, ..., drop=drop)
})

setMethod("eval", c("SolrExpression", "SolrFrame"),
          function (expr, envir, enclos) {
              undefer(transform(envir, x = .(expr)))$x
          })

setMethod("eval", c("SolrAggregateCall", "SolrFrame"),
          function (expr, envir, enclos) {
              df <- aggregate(envir, x = .(expr))
              grouped <- length(df) > 1L
              if (grouped) split(df$x, df[-length(df)]) else df$x
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setAs("Solr", "SolrFrame", function(from) {
  .SolrFrame(core(from), query(from), symbolFactory(from))
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

setAs("SolrFrame", "DocCollection",
      function(from) as.data.frame(from, optional=TRUE))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Summarizing
###

setGeneric("group", function(x, ...) standardGeneric("group"))

setMethod("group", "SolrFrame", function(x, by) {
              if (is.null(by)) {
                  return(x)
              }
              if (!is.formula(by)) {
                  stop("'by' must be NULL or a formula")
              }
              GroupedSolrFrame(x, by)
          })

summary.SolrFrame <- function(object, ...) {
    summary(object, ...)
}

setMethod("summary", "SolrFrame",
          function(object, maxsum = 7L,
                   digits = max(3L, getOption("digits") - 3L))
    {
        types <- fieldTypes(schema(core(object)), fieldNames(object))
        num <- vapply(types, is, logical(1L), "NumericField")
        p <- c(0.25, 0.5, 0.75)
        query <- query(object)
        fn <- fieldNames(object)
        for (f in fn[num])
            query <- facet(query, NULL, min(.field(f)),
                           mean(.field(f)), quantile(.field(f), p),
                           max(.field(f)))
        query <- facet(query, fn[!num], limit=maxsum)
        SolrSummary(facets(solr(x), query), fn, digits)
    })


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrFrame", function(object) {
  callNextMethod()
  out <- makePrettyMatrixForCompactPrinting(object)
  print(out, quote=FALSE, right=TRUE, max=length(out))
})
