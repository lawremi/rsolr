### =========================================================================
### SolrCore objects
### -------------------------------------------------------------------------

### Represents a Solr core; responsible for obtaining the Solr schema
### and processing Solr queries.

### We assume that the schema is relatively static and thus cache it
### during initialization. In theory though, we could retrieve it from
### the URI dynamically.

### TODO: consider getting the actual schema XML via the admin file
### handler, then we cast it to a media type, and parse it via
### dispatch. This will give us the fields in the original schema
### order. This at least lets the user/admin order the fields, instead
### of always forcing lexicographic order.

setClass("SolrCore",
         representation(uri="RestUri",
                        schema="SolrSchema",
                        version="package_version"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

SolrCore <- function(uri, ...) {
  if (!is(uri, "RestUri"))
    uri <- RestUri(uri, ...)
  else if (length(list(...)) > 0L)
    warning("arguments in '...' are ignored when uri is a RestUri")
  schema <- readSchema(uri)
  version <- readVersion(uri)
  new("SolrCore", uri=uri, schema=schema, version=version)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setGeneric("name", function(x) standardGeneric("name"))
setMethod("name", "ANY", function(x) x@name)
setMethod("name", "SolrCore", function(x) name(x@schema))

numFound <- function(x, query) {
    emptyQuery <- head(query, 0L)
    responseType(emptyQuery) <- "list"
    as.integer(eval(emptyQuery, x)$response$numFound)
}

setMethod("ndoc", "SolrCore", function(x, query = SolrQuery()) {
  numFound <- numFound(x, query)
  p <- prepareBoundsParams(params(query), numFound)
  min(numFound, p$rows)
})

schema <- function(x) x@schema

readLuke <- function(x) {
    processSolrResponse(read(x@uri$admin$luke, list(nTerms=0L, wt="json")))
}

globMatchMatrix <- function(x, patterns) {
    vapply(glob2rx(patterns), grepl, logical(length(x)), x)
}

extractByPatterns <- function(x, patterns) {
    m <- globMatchMatrix(x, patterns)
    ord <- order(max.col(m, ties.method="first"))
    x[ord][rowSums(m)[ord] > 0L]
}

orderFieldsBySchema <- function(x, schema) {
    schemaNames <- names(fields(schema))
    order(max.col(globMatchMatrix(x, schemaNames), ties.method="first"))
}

setMethod("fieldNames", "SolrCore",
          function(x, patterns = NULL, onlyStored = FALSE, onlyIndexed = FALSE,
                   includeStatic = FALSE)
              {
                  if (!is.null(patterns) &&
                      (!is.character(patterns) || any(is.na(patterns)))) {
                      stop("if non-NULL, 'patterns' must be a ",
                           "character vector without NAs")
                  }
                  if (!isTRUEorFALSE(includeStatic)) {
                      stop("'includeStatic' must be TRUE or FALSE")
                  }
                  if (!isTRUEorFALSE(onlyStored)) {
                      stop("'onlyStored' must be TRUE or FALSE")
                  }
                  if (!isTRUEorFALSE(onlyIndexed)) {
                      stop("'onlyIndexed' must be TRUE or FALSE")
                  }
                  ans <- tryCatch(names(readLuke(x)$fields),
                                  error = function(e) {
                                      warning("Luke request handler ",
                                              "unavailable --- ",
                                              "consider 'includeStatic=TRUE'")
                                      character()
                                  })
                  internal <- grepl("^_|____", ans)
                  ans <- ans[!internal]
                  if (includeStatic) {
                      f <- fields(schema(x))
                      ans <- union(ans, names(f)[!dynamic(f) & !hidden(f)])
                  }
                  if (onlyStored || onlyIndexed) {
                      f <- fields(schema(x), ans)
                      keep <-
                          (if (onlyStored) stored(f) else TRUE) &
                          (if (onlyIndexed) indexed(f) | docValues(f) else TRUE)
                      ans <- ans[keep]
                  }
                  ans <- ans[orderFieldsBySchema(ans, schema(x))]
                  if (!is.null(patterns)) {
                      ans <- extractByPatterns(ans, patterns)
                  }
                  ans
              })

setGeneric("version", function(x) standardGeneric("version"))

setMethod("version", "SolrCore", function(x) {
              x@version
          })

compatibleQuery <- function(x) {
    SolrQuery(version = if (version(x) >= "5.1") "5.1" else "")
}

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

### FIXME: multivalued fields not supported for CSV upload

setMethod("update", "SolrCore", function(object, value, commit=TRUE,
                                         atomic=FALSE, ...)
{
  if (!isTRUEorFALSE(commit)) {
    stop("'commit' must be TRUE or FALSE")
  }
  if (!isTRUEorFALSE(atomic)) {
    stop("'atomic' must be TRUE or FALSE")
  }
  if (is(value, "AsIs")) {
    class(value) <- setdiff(class(value), "AsIs")
  } else {
    value <- toUpdate(value, schema=schema(object), atomic=atomic)
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
setMethod("toUpdate", "ANY", function(x, schema, atomic=FALSE, ...) {
  x <- toSolr(x, schema, ...)
  if (atomic) {
    if (is.null(uniqueKey(schema))) {
      stop("modifying documents requires a 'uniqueKey' in the schema")
    }
    x <- unname(lapply(as(x, "DocList"), function(xi) {
      uk <- names(xi) == uniqueKey(schema)
      xi[is.na(xi)] <- list(NULL)
      xi[!uk] <- lapply(xi[!uk], function(f) list(set = f))
      xi
    }))
  }
  x
})

dropNAs <- function(x) {
  if (is.list(x)) {
    x[is.na(x)] <- NULL
  }
  x
}

setMethod("toUpdate", "list", function(x, schema, atomic=FALSE, ...) {
  if (is.data.frame(x)) {
    return(callNextMethod())
  }
  delete <- vapply(x, is.null, logical(1))
  if (!atomic && any(delete)) {
    x[!delete] <- lapply(toSolr(x[!delete], schema, ...),
                         function(xi) list(doc=xi))
    x[delete] <- lapply(names(x)[delete], function(nm) list(id=nm))
    setNames(x, ifelse(delete, "delete", "add"))
  } else {
    if (!atomic) {
      x <- lapply(x, dropNAs)
    }
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

readSchemaFromREST <- function(uri) {
  parseSchemaFromREST(processSolrResponse(read(uri$schema))$schema)
}

readSchemaXMLFile <- function(uri) {
  parseSchemaXML(read(uri$admin$file, file="schema.xml"))
}

readSchema <- function(uri) {
  tryCatch(readSchemaXMLFile(uri), error = function(e) {
    warning("Failed to retrieve schema XML file. Falling back to REST access. ",
            "Fields will be sorted lexicographically.")
    readSchemaFromREST(uri)
  })
}

readSystem <- function(uri) {
  processSolrResponse(read(uri$admin$system, wt="json"))
}

readVersion <- function(uri) {
  as.package_version(tryCatch({
      readSystem(uri)$lucene$"solr-spec-version"
  }, error = function(e) {
      stop("Failed to retrieve version, assuming 4.x")
      "4.x"
  }))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Summarizing
###

summary.SolrCore <- function(object,
                             of=setdiff(fieldNames(object),
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

setMethod("facets", "SolrCore",
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

processSolrResponse <- function(response, type = "json") {
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
            origin(response) <- .SolrList(envir, expr)
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

ngroup <- function(x, query) {
  params(query)$group.limit <- 0L
  groupings <- eval(query, x)$grouped
  vapply(groupings, function(g) length(g$groups), integer(1L))
}

resultLength <- function(x, query) {
  ans <- if (identical(params(query)$group, "true"))
    ngroup(x, query)
  else numFound(x, query)
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
  invisible(as.integer(processSolrResponse(resp$responseHeader$status)))
}
setMethod("commit", "SolrCore", commit_SolrCore)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrCore", function(object) {
  cat("SolrCore object\n")
  cat("name:", name(object), "\n")
  cat("ndoc:", ndoc(object), "\n")
  cat("schema:", length(fields(schema(object))), "fields\n")
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities
###

setMethod("purgeCache", "SolrCore", function(x) {
  purgeCache(x@uri)
  invisible(x)
})
