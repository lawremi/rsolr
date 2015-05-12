### =========================================================================
### SolrList objects
### -------------------------------------------------------------------------
###
### High-level object that represents a Solr core as a list, with benefits
###

### TODO: consider depending on S4Vectors and extending List
setClass("SolrList", contains="Solr")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

.SolrList <- function(core, query=SolrQuery()) {
  new("SolrList", core=core, query=query)
}

SolrList <- function(uri, ...) {
  .SolrList(SolrCore(uri, ...))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setMethod("length", "SolrList", function(x) {
  ndoc(x)
})

setMethod("names", "SolrList", function(x) {
  ids(x)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### CREATE/UPDATE/DELETE
###

## What to do with x[] <- objs?  Is it a replace-all, or do we assume
## the IDs are inherent in the object as the default and add them? It
## seems to make sense for [<- to take the ids from value for the
## default of 'i'. Then the value of NULL is a special case, where 'i'
## defaults to everything. But that is very inconsistent with existing
## behavior in R. Instead, we need an extra argument, 'insert', to
## indicate that we do not want to replace the existing data.

setReplaceMethod("[", "SolrList", function(x, i, j, insert=FALSE, ..., value) {
  if (!isTRUEorFALSE(insert)) {
    stop("'insert' must be TRUE or FALSE")
  }
  if (insert && is.null(value)) {
    stop("cannot insert a 'value' of NULL")
  }
  if (!insert && missing(i) && missing(j)) {
    core <- delete(core(x), query(x), ...)
  }
  if (!is.null(value) || !missing(i) || !missing(j)) {
    atomic <- FALSE
    if (!missing(j)) {
      if (!is.character(j) || any(is.na(j))) {
        stop("'j' must be character without NAs")
      }
      if (is.null(uniqueKey(schema(core(x))))) {
        stop("modifying documents requires a 'uniqueKey' in the schema")
      }
      atomic <- TRUE
      if (is.null(value)) {
        value <- list(setNames(rep(list(NULL), length(j)), j))
      }
      if (missing(i) && !(uniqueKey(schema(core(x))) %in% j)) {
        i <- ids(x)
      }
    }
    if (is.null(value)) {
      value <- list(NULL)
    }
    value <- as(value, "DocCollection")
    if (!missing(j)) {
      fieldNames(value) <- j
    }
    if (!missing(i)) {
      if (length(i) == 0L) {
        return(x)
      }
      if (ndoc(value) < length(i))
        value <- value[recycleVector(seq_len(NROW(value)), length(i)),]
      ids(value) <- i
    }
    core <- update(core(x), value, atomic=atomic, ...)
  }
  initialize(x, core=purgeCache(core))
})

setReplaceMethod("[[", "SolrList", function(x, i, j, ..., value) {
  if (!missing(j))
    warning("argument 'j' is ignored")
  if (missing(i))
    stop("argument 'i' cannot be missing")
  x[i, ...] <- list(value)
  x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### READ
###

setMethod("[[", "SolrList", function(x, i, j, ...) {
  if (missing(i))
    stop("'i' cannot be missing")
  if (!isSingleString(i))
    stop("'i' must be a single, non-NA string")
  docs <- as.list(if (!missing(j)) x[i, j, ..., drop=FALSE] else x[i, ...])
  if (length(docs) == 0L) {
    NULL
  } else {
    docs[[1L]]
  }
})

setMethod("[", "SolrList", function(x, i, j, ..., drop = TRUE) {
  if (!isTRUEorFALSE(drop)) {
    stop("'drop' must be TRUE or FALSE")
  }
  query <- query(x)
  if (!missing(i)) {
    if (!is.character(i) || any(is.na(i))) {
      stop("'i' must be character, without any NAs")
    }
    if (is.null(uniqueKey(schema(core(x))))) {
      stop("retrieving a document by ID requires a 'uniqueKey' in the schema")
    }
    query <- subset(query, .field(uniqueKey(schema(core(x)))) %in% .(i))
  }
  if (!missing(j)) {
    query <- subset(query,
                    fields = if (!missing(i))
                               union(j, uniqueKey(schema(core(x))))
                             else j)
  }
  query(x) <- query
  readColumn <- drop && !missing(j) && length(j) == 1L
  if (readColumn) {
    if (missing(i) && (is(j, "Symbol") || !is.null(symbolFactory(x)))) {
      if (!is(j, "Symbol"))
        j <- symbolFactory(x)(j)
      ans <- Promise(j, x)   
    }  else {
      ans <- as.data.frame(x, row.names=!missing(i))
      ## ensure things are in the correct order
      if (!missing(i)) {
        ans <- ans[i,j]
      } else {
        ans <- ans[,j]
      }
    }
    ans
  } else {
    readDoc <- drop && !missing(drop) && ndoc(x) == 1L
    if (readDoc) {
      as.list(x)[[1L]]
    } else {
      x
    }
  }
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setAs("Solr", "SolrList", function(from) {
  .SolrList(core(from), query(from))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrList", function(object) {
  callNextMethod()
  if (length(object) > 0L) {
    showDoc(as.list(head(object, 1L))[[1L]], "First document")
  }
})
