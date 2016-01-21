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

.SolrList <- function(core, query=SolrQuery(), symbolFactory=NULL) {
  new("SolrList", core=core, query=query, symbolFactory=symbolFactory)
}

SolrList <- function(uri, ...) {
  .SolrList(SolrCore(uri), SolrQuery(...))
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

## Do we want to require a writable() cast before allowing the user to
## write to the database (assuming it is publicly modifiable)?
## Currently, [<-,SolrList always writes, while [<-,SolrPromise is a
## dynamic override.

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
        i <- unlist(ids(x), use.names=FALSE)
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
      if (is(i, "Promise")) {
        i <- ids(x[i])
      }
      if (length(i) == 0L) {
        return(x)
      }
      if (is.null(uniqueKey(schema(core(x))))) {
        stop("'i' must be missing if there is no unique key in the schema")
      }
      if (!is.character(i)) {
        i <- ids(x)[i]
      }
      if (any(is.na(i))) {
        stop("'i' resolves to one or more NAs")
      }
      if (ndoc(value) < length(i)) {
          ind <- recycleIntegerArg(seq_len(NROW(value)), "value", length(i))
          value <- value[ind,]
      }
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

MAX_LUCENE_QUERY_LENGTH <- 1024

.Solr_2DBracket <- function(x, i, j, ..., drop = TRUE) {
  if (!isTRUEorFALSE(drop)) {
    stop("'drop' must be TRUE or FALSE")
  }
  query <- query(x)
  readColumn <- drop && !missing(j) && length(j) == 1L
  lazyReadColumn <- readColumn &&
      (is(j, "Symbol") || !is.null(symbolFactory(x)))
  if (!missing(i)) {
### FIXME: lazy 'i' does not handle NAs (was fine for subset(), but not '[')
    lazyI <- is(i, "Promise") || is(i, "Expression")
    if (is.integer(i) && isTRUE(all(diff(i) == 1L))) {
        query <- window(query, head(i, 1L), tail(i, 1L))
        lazyI <- TRUE
    } else if ((!readColumn || lazyReadColumn) && !lazyI) {
        if (is.null(uniqueKey(schema(core(x))))) {
            stop("retrieving a doc by ID requires a 'uniqueKey' in the schema")
        }
        if (!is.character(i)) {
            i <- ids(undefer(x))[i]
        }
        if (any(is.na(i))) {
            stop("'i' resolved to one or more NAs")
        }
        tooManyIds <- length(i) > MAX_LUCENE_QUERY_LENGTH
        if (!(lazyReadColumn && tooManyIds)) {
            if (tooManyIds) {
                warning("more than ", MAX_LUCENE_QUERY_LENGTH,
                        " ids requested, expect Lucene to break ",
                        "(try using a promise).")
            }
            query <- subset(query, .field(uniqueKey(schema(core(x)))) %in% .(i))
            lazyI <- TRUE
        }
    } else if (lazyI) {
      query <- subset(query, .(i))
    }
  }
  if (lazyReadColumn && (missing(i) || lazyI)) {
      query(x) <- query
      if (!is(j, "Symbol"))
          j <- symbolFactory(x)(j)
      return(Promise(j, x))
  }
  if (!missing(j)) {
    query <- subset(query,
                    fields = if (!missing(i))
                               union(j, uniqueKey(schema(core(x))))
                             else j)
  }
  query(x) <- query  
  if (readColumn) {
      ans <- as.data.frame(x, row.names=!missing(i))
      ## ensure things are in the correct order
      if (!missing(i)) {
        ans[i,j]
      } else {
        ans[,j]
      }
  } else {
    readDoc <- drop && !missing(drop) && identical(ndoc(x), 1L)
    if (readDoc) {
      ans <- as.list(x)
      if (is(ans, "DocList"))
        ans <- ans[[1L]]
      ans
    } else {
      x
    }
  }
}

setMethod("[", "SolrList", .Solr_2DBracket)

setMethod("unique", "SolrList", function (x, incomparables = FALSE) {
              unid(as(callNextMethod(), "DocList"))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setAs("Solr", "SolrList", function(from) {
  .SolrList(core(from), query(from), symbolFactory(from))
})

setAs("SolrList", "DocCollection", function(from) as.list(from))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrList", function(object) {
  callNextMethod()
  if (length(object) > 0L) {
    showDoc(as.list(head(object, 1L))[[1L]], "First document")
  }
})
