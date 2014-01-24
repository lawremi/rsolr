### =========================================================================
### SolrCore objects
### -------------------------------------------------------------------------

### Represents a Solr core; responsible for obtaining the Solr schema
### and processing Solr queries.

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
  schema <- SolrSchema(read(uri$schema)$schema)
  new("SolrCore", uri=uri, schema=schema)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setGeneric("name", function(x) standardGeneric("name"))
setMethod("name", "ANY", function(x) x@name)
setMethod("name", "SolrCore", function(x) name(x@schema))

setMethod("length", "SolrCore", function(x) {
  eval(nrow(SolrQuery()), x)
})

schema <- function(x) x@schema

setMethod("purgeCache", "SolrCore", function(x) {
  purgeCache(x@uri)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### CREATE/UPDATE/DELETE
###

## What to do with x[] <- objs?  Is it a replace-all, or do we assume
## the IDs are inherent in the object as the default and add them? The
## latter is probably a much more common use-case. If we choose the
## latter, then how does one delete records? x[i] <- NULL would work
## for named records, but what about deleting everything? That is not
## too hard to do with update() directly...

setReplaceMethod("[", "SolrCore", function(x, i, j, ..., value) {
  if (!missing(j)) {
    warning("argument 'j' is ignored")
  }
  if (is.null(value)) {
    if (missing(i)) {
      stop("'i' must be specified when 'value' is NULL")
    }
    value <- list(NULL)
  }
  value <- as(value, "DocCollection")
  if (!missing(i)) {
    value <- value[recycleVector(seq_len(NROW(value)), length(i)),]
    ids(value) <- i
  }
  update(x, value, ...)
})

setReplaceMethod("[", c("SolrCore", "SolrQuery"), function(x, i, j, ..., value) {
  if (is.null(value)) {
    if (!isSimpleQuery(i)) {
      stop("query 'i' must only specify 'q' parameter")
    }
    update(x, I(list(delete=list(query=i$q))))
  } else {
    stop("unable to replace by query")
  }
})

setReplaceMethod("$", "SolrCore", function(x, name, value) {
  x[[name]] <- value
  x
})

setReplaceMethod("[[", "SolrCore", function(x, i, j, ..., value) {
  if (!missing(j))
    warning("argument 'j' is ignored")
  if (missing(i))
    stop("argument 'i' cannot be missing")
  x[i, ...] <- list(value)
  x
})

### TODO: support multi-valued csv fields by adding
### f.[field].split=true, probably via generic updateParams(value).

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
  query.params <- list()
  if (commit) {
    query.params <- commitQueryParams(...)
  }
  create(object@uri$update, media, query.params)
  invisible(object)
})

## setGeneric("toUpdate", function(x) standardGeneric("toUpdate"))
## setMethod("toUpdate", "ANY", identity)
## setMethod("toUpdate", "DocList", function(x) {
##   delete <- vapply(x, is.null, logical(1))
##   if (any(delete)) {
##     x[!delete] <- lapply(x[!delete], function(xi) list(doc=xi))
##     x[delete] <- lapply(names(x)[delete], function(nm) list(id=nm))
##     setNames(x, ifelse(delete, "delete", "add"))
##   }
##   x
## })

setGeneric("toUpdate", function(x, ...) standardGeneric("toUpdate"))
setMethod("toUpdate", "ANY", toSolr)
setMethod("toUpdate", "DocList", function(x, ...) {
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
  simple <- SolrQuery()
  x$q <- simple$q
  identical(x, simple)
}

setMethod("delete", "SolrCore", function(x, which = SolrQuery(), ...) {
  x[which] <- NULL
  invisible(x)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### READ
###

localParams <- function(...) {
  args <- list(...)
  paste0("{!", paste0(names(args), "=", args, collapse=" "), "}")
}

setMethod("$", "SolrCore", function(x, name) {
  x[[name]]
})

setMethod("[[", "SolrCore", function(x, i, j, ...) {
  if (!missing(j) || length(list(...)) > 0L)
    warning("argument 'j' and arguments in '...' are ignored")
  if (missing(i))
    stop("'i' cannot be missing")
  if (!isSingleString(i))
    stop("'i' must be a single, non-NA string")
  docs <- x[i, ...]
  if (length(docs) == 0L) {
    NULL
  } else {
    docs[[1]]
  }
})

setMethod("[", "SolrCore", function(x, i, j, ..., drop = TRUE) {
  if (!isTRUE(drop)) {
    stop("'drop' must be TRUE")
  }
  if (!missing(j)) {
    warning("argument 'j' is ignored")
  }
  uk <- uniqueKey(schema(x))
  if (missing(i)) {
    q <- "*:*"
  } else {
    if (!is.character(i) || any(is.na(i))) {
      stop("'i' must be character, without any NAs")
    }
    q <- paste0(localParams(q.op="OR", df=uk),
                paste0("(", paste(i, collapse=" "), ")"))
  }
### TODO: add a setting on SolrCore that coerces to data.frame here?
  read(x, q=q, ...)
})

setMethod("read", "SolrCore", function(x, q="*:*", ...) {
  fromSolr(read(x@uri$select, q=q, wt="json", ...)$response$docs, schema(x))
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
  invisible(as.integer(resp$responseHeader$status))
}
setMethod("commit", "SolrCore", commit_SolrCore)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrCore", function(object) {
  cat("SolrCore object\n")
  cat("name:", name(object), "\n")
  cat("length:", length(object), "\n")
  cat("schema:", length(fieldInfo(object@schema)), "fields\n")
})
