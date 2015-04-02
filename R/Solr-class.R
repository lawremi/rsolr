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
### - summary operations (summary, aggregate, xtabs) and
### - coercion (as.list, as.data.frame, [,drop=TRUE]).
### Only endomorphic operations are lazy:
### - subset, [,drop=FALSE]
### - transform
### - sort

setClass("Solr",
         representation(core="SolrCore",
                        query="SolrQuery"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

.Solr <- function(core, query=SolrQuery()) {
  new("Solr", core=core, query=query)
}

Solr <- function(uri, ...) {
  .Solr(SolrCore(uri, ...))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

core <- function(x) x@core

query <- function(x) x@query
`query<-` <- function(x, value) {
  x@query <- value
  x
}

setMethod("dim", "Solr", function(x) {
  c(nrow(x), ncol(x))
})

setMethod("ncol", "Solr", function(x) {
  fl <- params(query(x))$fl
  glob <- grepl("*", fl, fixed=TRUE)
  if (any(glob)) {
    f <- fields(schema(core(x)))
    if (any(dynamic(f))) {
      NA_integer_ # we punt, instead of trying to resolve globs against globs
    } else {
      sum(stored(f) & !hidden(f))
    }
  } else {
    length(fl)
  }
})

setMethod("nrow", "Solr", function(x) {
  length(x)
})

setMethod("length", "Solr", function(x) {
  as.integer(eval(nrow(query(x)), core(x))$response$numFound)
})

setMethod("ngroup", "Solr", function(x) {
  groupings <- eval(ngroup(query(x)), core(x))$grouped
  vapply(groupings, function(g) length(g$groups), integer(1L))
})

setMethod("names", "Solr", function(x) {
  x[,uniqueKey(schema(core(x)))]
})

setMethod("staticFieldNames", "Solr", function(x) {
  staticFieldNames(schema(core(x)))
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

setReplaceMethod("[", "Solr", function(x, i, j, insert=FALSE, ..., value) {
  if (!missing(j)) {
    warning("argument 'j' is ignored")
  }
  if (!isTRUEorFALSE(insert)) {
    stop("'insert' must be TRUE or FALSE")
  }
  if (insert && is.null(value)) {
    stop("cannot insert a 'value' of NULL")
  }
  if (!insert && missing(i)) {
    core <- delete(core(x), query(x), ...)
  }
  if (!is.null(value) || !missing(i)) {
    if (is.null(value)) {
      value <- list(NULL)
    }
    value <- as(value, "DocCollection")
    if (!missing(i)) {
      value <- value[recycleVector(seq_len(NROW(value)), length(i)),]
      ids(value) <- i
    }
    core <- update(core(x), value, ...)
  }
  initialize(x, core=purgeCache(core))
})

setReplaceMethod("$", "Solr", function(x, name, value) {
  x[[name]] <- value
  x
})

setReplaceMethod("[[", "Solr", function(x, i, j, ..., value) {
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

setMethod("$", "Solr", function(x, name) {
  x[[name]]
})

setMethod("[[", "Solr", function(x, i, j, ...) {
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

setMethod("[", "Solr", function(x, i, j, ..., drop = TRUE) {
  if (!isTRUEorFALSE(drop)) {
    stop("'drop' must be TRUE or FALSE")
  }
  query <- query(x)
  if (!missing(i)) {
    if (!is.character(i) || any(is.na(i))) {
      stop("'i' must be character, without any NAs")
    }
    query <- subset(query, as.name(.(uniqueKey(schema(core(x))))) %in% .(i))
  }
  if (!missing(j)) {
    query <- subset(query,
                    fields = if (!missing(i))
                               union(j, uniqueKey(schema(core(x))))
                             else j)
  }
  query(x) <- query
  read <- drop && !missing(j) && length(j) == 1L
  if (read) {
    ans <- as.data.frame(x)
    ## ensure things are in the correct order
    if (!missing(i)) {
      ans <- ans[i,j]
    } else {
      ans <- ans[,j]
    }
    ans
  } else {
    x
  }
})

setMethod("subset", "Solr", function(x, ...) {
  query(x) <- subset(query(x), ...)
  x
})

head.Solr <- function (x, n = 6L, ...) {
  query(x) <- head(query(x), n, ...)
  x
}
setMethod("head", "Solr", head.Solr)

tail.Solr <- function (x, n = 6L, ...) {
  query(x) <- tail(query(x), n, ...)
  x
}
setMethod("tail", "Solr", tail.Solr)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Transforming
###

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
##

summary.Solr <- function(object, ...)
{
  summary(core(object),
          query = summary(query(object), schema(core(object)), ...))
}

setMethod("summary", "Solr", summary.Solr)

setMethod("xtabs", "Solr",
          function(formula, data,
                   subset, sparse = FALSE, 
                   na.action, exclude = NA,
                   drop.unused.levels = FALSE)
          {
            if (!all(names(match.call())[-1L] %in%
                     c("formula", "data", "exclude"))) {
              stop("all args except 'formula', 'data' and 'exclude' are ignored")
            }
            q <- facet(query(data), formula, useNA=is.null(exclude))
            facet(core(data), q)[[1L]]
          })

setMethod("aggregate", "formula", function(x, ...) {
  aggregateByFormula(x, ...)
})

setGeneric("aggregateByFormula",
           function(formula, data, ...) standardGeneric("aggregateByFormula"),
           signature="data")

setMethod("aggregateByFormula", "ANY", stats:::aggregate.formula)

setMethod("aggregateByFormula", "Solr", function(formula, data, FUN, ...) {
  FUN(SolrAggregation(formula, data), ...)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

as.data.frame.Solr <- function(x, row.names = NULL, optional = FALSE)
{
  if (!isTRUEorFALSE(optional)) {
    stop("'optional' must be TRUE or FALSE")
  }
  df <- read(core(x), query(x), as="data.frame")
  if (!optional) {
    colnames(df) <- make.names(colnames(df), unique = TRUE)
  }
  if (!is.null(row.names)) {
    rownames(df) <- row.names
  }
  df
}

setMethod("as.data.frame", "Solr", as.data.frame.Solr)

as.list.Solr <- function(x) {
  read(core(x), query(x))
}

setMethod("as.list", "Solr", as.list.Solr)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "Solr", function(object) {
  cat("Solr object\n")
  cat("core: '", name(core(object)), "' with ", nrow(object), " rows and ",
      length(fields(schema(core(object)))), " fields\n", sep="")
  query <- as.character(query(object))
  defaults <- as.character(SolrQuery())
  drop <- which(defaults[names(query)] == query)
  if (length(drop) > 0L) {
    query <- query[names(query) %in% names(query)[-drop]]
  }
  labeled.query <- paste0(names(query), "='", query, "'")
  cat(BiocGenerics:::labeledLine("query", labeled.query))
})
