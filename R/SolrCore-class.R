### =========================================================================
### SolrCore objects
### -------------------------------------------------------------------------

### Represents a Solr core; responsible for obtaining the Solr schema
### and processing Solr queries.

### We assume that the schema is relatively static and thus cache it
### during initialization. In theory though, we could retrieve it from
### the URI dynamically.

setClass("SolrCore",
         representation(uri="RestUri",
                        schema="SolrSchema"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

SolrCore <- function(uri, ...) {
  if (!is(uri, "RestUri"))
    uri <- RestUri(uri, ...)
  else if (length(list(...)) > 0L)
    warning("arguments in '...' are ignored when uri is a RestUri")
  schema <- parseSchema(read(uri$schema)$schema)
  new("SolrCore", uri=uri, schema=schema)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setGeneric("name", function(x) standardGeneric("name"))
setMethod("name", "ANY", function(x) x@name)
setMethod("name", "SolrCore", function(x) name(x@schema))

setMethod("nrow", "SolrCore", function(x) {
  nrow(.Solr(x))
})

schema <- function(x) x@schema

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### CREATE/UPDATE/DELETE
###

setGeneric("updateParams", function(x) standardGeneric("updateParams"))

setMethod("updateParams", "ANY", function(x) {
  list()
})

setMethod("updateParams", "data.frame", function(x) {
  list(map="NA:")
})

### FIXME: multivalued fields not supported for CSV upload; Solr needs Avro...

setMethod("update", "SolrCore", function(object, value, commit=TRUE, ...) {
  if (!isTRUEorFALSE(commit)) {
    stop("'commit' must be TRUE or FALSE")
  }
  if (is(value, "AsIs")) {
    class(value) <- setdiff(class(value), "AsIs")
  } else {
    value <- toUpdate(value, schema(object))
  }
  media <- as(value, "Media")
  query.params <- updateParams(value)
  if (commit) {
    query.params <- c(query.params, commitQueryParams(...))
  }
  create(object@uri$update, media, query.params)
  invisible(object)
})

setGeneric("toUpdate", function(x, ...) standardGeneric("toUpdate"))
setMethod("toUpdate", "ANY", toSolr)
setMethod("toUpdate", "list", function(x, ...) {
  delete <- vapply(x, is.null, logical(1))
  if (any(delete)) {
    x[!delete] <- lapply(toSolr(x[!delete], ...), function(xi) list(doc=xi))
    x[delete] <- lapply(names(x)[delete], function(nm) list(id=nm))
    setNames(x, ifelse(delete, "delete", "add"))
  } else {
    callNextMethod()
  }
})

isSimpleQuery <- function(x) {
  params(x)$fq <- NULL
  identical(x, SolrQuery())
}

setMethod("delete", "SolrCore", function(x, which = SolrQuery(), ...) {
  if (!isSimpleQuery(which)) {
    warning("delete() cannot handle 'which' more complex than ",
            "'subset(SolrQuery(), [expr])'")
  }
  query <- params(which)$fq
  if (is.null(query)) {
    query <- params(which)$q
  }
  invisible(update(x, I(list(delete=list(query=query)))))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### READ
###

setMethod("read", "SolrCore",
          function(x, query=SolrQuery(), as=c("list", "data.frame"))
          {
            if (!is(query, "SolrQuery")) {
              stop("'query' must be a SolrQuery")
            }
            responseType(query) <- match.arg(as)
            fromSolr(eval(query, x))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Summarizing
###

summary.SolrCore <- function(object,
                             of=setdiff(staticFieldNames(schema(object)),
                               uniqueKey(schema(object))),
                             query=summary(SolrQuery(), object, of), ...)
{
  if (!is(query, "SolrQuery")) {
    stop("'query' must be a SolrQuery")
  }
  if (!missing(of) && !missing(query)) {
    query <- summary(query, object, of, ...)
  }
  SolrSummary(eval(query, object))
}

setMethod("summary", "SolrCore", summary.SolrCore)

setMethod("facet", c("SolrCore", "SolrQuery"),
          function(x, by) {
            facets(summary(x, query=by))
          })

setMethod("stats", c("SolrCore", "SolrQuery"),
          function(x, which) {
            if (!enablesStats(which)) {
              stop("'which' does not enable stats")
            }
            stats(summary(x, query=which))
          })

setMethod("groups", c("SolrCore", "SolrQuery"), function(x, by=SolrQuery()) {
  if (!is(by, "SolrQuery")) {
    stop("'by' must be a SolrQuery")
  }
  fromSolr_grouped(eval(by, x), schema(x))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Query Evaluation
###

processSolrResponse <- function(response, type) {
  ## some SOLR instances return text/plain for responses...
  ## this also happens for errors
  if (is.character(response)) {
    mediaType <- switch(type,
                        json="application/json",
                        csv="text/csv",
                        xml="application/xml")
    media <- new(mediaType, response)
    response <- as(media, mediaTarget(media))
  }
  response
}

## Unfortunately Solr does not describe errors with CSV output (!)
## So we reissue the query with JSON when one occurs
SolrErrorHandler <- function(core, query) {
  function(e) {
    if (!is(e, "HTTPError")) {
      stop(e)
    }
    if (params(query)$wt == "json") {
      response <- processSolrResponse(attr(e, "body"), params(query)$wt)
      stop("[", response$error$code, "] ", response$error$msg,
           if (!is.null(response$error$trace))
             paste("\nJava stacktrace:\n", response$error$trace),
           call.=FALSE)
    } else {
      params(query)$wt <- "json"
      eval(query, core)
    }
  }
}

origin <- function(x) attr(x, "origin")

`origin<-` <- function(x, value) {
  attr(x, "origin") <- value
  x
}

setMethod("eval", c("SolrQuery", "SolrCore"),
          function (expr, envir, enclos)
          {
            params <- prepareQueryParams(envir, expr)
            expected.type <- params["wt"]
            response <- tryCatch(read(envir@uri$select, params),
                                 error = SolrErrorHandler(envir, expr))
            response <- processSolrResponse(response, expected.type)
            origin(response) <- .Solr(envir, expr)
            response
          })

prepareBoundsParams <- function(p, nrows) {
  ans_start <- 0L
  ans_rows <- .Machine$integer.max

  stopifnot(identical(length(p$start), length(p$rows)))
  
  for (i in seq_along(p$start)) {
    head_minus <- p$rows[i] < 0L
    if (isTRUE(head_minus)) {
      ans_rows <- min(ans_rows, nrows) + p$rows[i]
    } else {
      ans_rows <- min(ans_rows, p$rows[i], na.rm=TRUE)
    }
    tail_plus <- p$start[i] < 0L
    if (tail_plus) {
      ans_start <- ans_start + min(ans_rows, nrows) + p$start[i]
      if (is.na(p$rows[i]))
        ans_rows <- min(ans_rows, abs(p$start[i]))
    } else {
      ans_start <- ans_start + p$start[i]
      if (is.na(p$rows[i]))
        ans_rows <- min(ans_rows, nrows) - p$start[i]
    }
  }
  
  p$start <- ans_start
  p$rows <- ans_rows

  p
}

resultLength <- function(x, query) {
  solr <- .Solr(x, query)
  ans <- if (identical(params(query)$group, "true"))
    ngroup(solr)
  else nrow(solr)
  if (length(ans) > 1L) {
    warning("ambiguous result length (multiple groupings)")
  }
  ans
}

prepareQueryParams <- function(x, query) {
  params(query) <- prepareBoundsParams(params(query), resultLength(x, query))
  if (is.null(responseType(query)))
    responseType(query) <- "list"
  as.character(query)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Other commands
###

setGeneric("commit", function(x, ...) standardGeneric("commit"))

commitQueryParams <- function(...) {
  def <- as.function(c(formals(commit_SolrCore)[-1], list(NULL)))
  call.args <- as.list(match.call(def))[-1]
  args <- formals(def)
  args[names(call.args)] <- call.args
  args <- do.call(c, c(commit="true", lapply(args, eval, args)))
  tolower(args)
}

commit_SolrCore <- function(x, waitSearcher=TRUE, softCommit=FALSE,
                            expungeDeletes=FALSE,
                            optimize=TRUE, maxSegments=if (optimize) 1L)
{
  args <- tail(as.list(match.call()), -2)
  resp <- read(x@uri$update, do.call(commitQueryParams, args), wt="json")
  invisible(as.integer(resp$responseHeader$status))
}
setMethod("commit", "SolrCore", commit_SolrCore)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrCore", function(object) {
  cat("SolrCore object\n")
  cat("name:", name(object), "\n")
  cat("nrow:", nrow(object), "\n")
  cat("schema:", length(fields(schema(object))), "fields\n")
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities
###

setMethod("purgeCache", "SolrCore", function(x) {
  purgeCache(x@uri)
  invisible(x)
})
