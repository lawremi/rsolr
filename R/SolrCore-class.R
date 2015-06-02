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

setMethod("name", "ANY", function(x) x@name)
setMethod("name", "SolrCore", function(x) name(x@schema))

numFound <- function(x, query) {
    emptyQuery <- head(query, 0L)
    responseType(emptyQuery) <- "list"
    ndoc(eval(emptyQuery, x))
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
  query <- eval(params(which)$fq, x)
  if (is.null(query)) {
    query <- params(which)$q
  }
  invisible(update(x, I(list(delete=list(query=as.character(query))))))
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
            as <- match.arg(as)
            responseType(query) <- if (grouped(query)) "list" else as
            as(docs(eval(query, x)), as, strict=FALSE)
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

setGeneric("facets", function(x, ...) standardGeneric("facets"))

setMethod("facets", "SolrCore", function(x, by, ...) {
              facets(eval(by, x), ...)
          })

setGeneric("groupings", function(x, ...) standardGeneric("groupings"))

setMethod("groupings", "SolrCore", function(x, by, ...) {
              groupings(eval(by, x), ...)
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

setMethod("eval", c("SolrQuery", "SolrCore"),
          function (expr, envir, enclos)
          {
            if (is.null(responseType(expr)))
              responseType(expr) <- "list"
            params <- translate(expr, core=envir)
            expected.type <- params["wt"]
            response <- tryCatch(read(envir@uri$select, params),
                                 error = SolrErrorHandler(envir, expr))
            response <- processSolrResponse(response, expected.type)
            convertSolrQueryResponse(response, envir, expr)
          })

setGeneric("ngroup", function(x, ...) standardGeneric("ngroup"))

setMethod("ngroup", "SolrCore", function(x, query) {
              params(query)$group.limit <- 0L
              ngroup(eval(query, x))
          })

resultLength <- function(x, query) {
  ans <- if (grouped(query))
    ngroup(x, query)
  else numFound(x, query)
  if (length(ans) > 1L) {
    warning("ambiguous result length (multiple groupings)")
  }
  ans
}

setGeneric("convertSolrQueryResponse",
           function(x, core, query) standardGeneric("convertSolrQueryResponse"),
           signature=c("x"))

setMethod("convertSolrQueryResponse", "ANY", function(x, core, query) {
              fromSolr(x, schema(core), query)
          })

setMethod("convertSolrQueryResponse", "list", function(x, core, query) {
              ListSolrResult(x, core, query)
          })

setMethod("eval", c("TranslationRequest", "SolrCore"),
          function (expr, envir, enclos) {
              if (!missing(enclos)) {
                  warning("'enclos' is ignored")
              }
              translate(expr@src, expr@target, envir)
          })

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
  invisible(as.integer(processSolrResponse(resp)$responseHeader$status))
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
