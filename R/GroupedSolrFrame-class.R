### =========================================================================
### GroupedSolrFrame objects
### -------------------------------------------------------------------------
###
### Extends SolrFrame with the notion of a grouping. Conceptually,
### each row corresponds to a group, and each column is a list, split
### according to the interaction of the grouping factors.
###
### Note that subset(), sort(), transform(), etc, operate on the
### internal rows, because conceptually the indexes and replacement
### values are themselves lists.
###
### Eventually, we might want special grouped Promise subclasses,
### where is.list(promise) returns TRUE. But for now we can get away
### with basic promises.

setClass("GroupedSolrFrame",
         representation(grouping="formula"),
         contains="SolrFrame",
         validity=function(object) {
             if (length(grouping(object)) == 3L)
                 "grouping formula cannot have an LHS"
         })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###
### Internal, users construct via group,Solrframe().
###

GroupedSolrFrame <- function(frame, grouping) {
    new("GroupedSolrFrame", defer(frame), grouping=grouping)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

grouping <- function(x) x@grouping

setMethod("ndoc", "GroupedSolrFrame", function(x) {
              xtabs(data=x)
          })

setMethod("rownames", "GroupedSolrFrame", function(x) {
              x <- x[all.vars(grouping(x))]
              groups <- as.data.frame(as(x, "SolrFrame", strict=TRUE))
              do.call(paste, c(groups, sep="."))
          })

setMethod("nrow", "GroupedSolrFrame", function(x) {
              length(ndoc(x))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### CREATE/UPDATE/DELETE
###

setReplaceMethod("[", "GroupedSolrFrame", function(x, i, j, ..., value) {
                     normValue <- function(v, nd) {
                         if (all(lengths(v) == 1L)) {
                             rep(unlist(v, use.names=FALSE), nd)
                         } else {
                             needrep <- lengths(sv) != nd
                             v[needrep] <- mapply(rep, v[needrep],
                                                  length.out=nd[needrep],
                                                  SIMPLIFY=FALSE)
                             unlist(v, use.names=FALSE)
                         }
                     }
                     if (is.data.frame(value)) {
                         if (!all(vapply(value, is.list, logical(1L)))) {
                             stop("all columns in value must be lists")
                         }
                         value <- lapply(value, normValue, ndoc(x))
                     } else {
                         if (!is.list(value)) {
                             stop("'value' must be a list, ",
                                  "or a data.frame of lists")
                         }
                         value <- normValue(value, ndoc(x))
                     }
                     if (!missing(i)) {
                         if (is.list(i) && !is.null(names(i)) &&
                             !all(vapply(i, is.character, logical(1L)))) {
                             x <- x[names(i),]
                         } else if (!is.list(i)) {
                             i <- ids(x)[i]
                         }
                         i <- unlist(i, use.names=FALSE)
                     }
                     callNextMethod()
                 })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### READ
###

setMethod("[", "SolrFrame", function(x, i, j, ..., drop = TRUE) {
              if (!missing(i) && !(is(i, "Promise") || is(i, "Expression"))) {
                  if (is.list(i) && !is.null(names(i)) &&
                      !all(vapply(i, is.character, logical(1L)))) {
                      x <- x[names(i),]
                  } else if (!is.list(i)) {
                      i <- ids(x)[i]
                  }
                  i <- unlist(i, use.names=FALSE)
              }
              callNextMethod()
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Summarizing
###
### Any specified groupings are interacted with the current grouping.
###

mergeGrouping <- function(x, y) {
    lhs <- if (length(y) == 3L) y[[2L]]
    vars <- union(colnames(attr(terms(x), "factors")),
                  colnames(attr(terms(y), "factors")))
    as.formula(paste(lhs, "~", paste(vars, collapse="+")))
}

setMethod("group", "GroupedSolrFrame", function(x, by) {
              if (is.null(by)) {
                  return(x)
              }
              if (!is.formula(by)) {
                  stop("'by' must be NULL or a formula")
              }
              initialize(x, grouping=mergeGrouping(grouping(x), by))
          })

### FIXME: We rely on the Solr Group component, which uses the global
### output restriction, but this only works for single term groupings.

setMethod("window", "GroupedSolrFrame", function (x, ...) {
              query(x) <- group(query(x), grouping(x))
              callNextMethod()
          })

setMethod("head", "GroupedSolrFrame", function (x, n = 6L, ...) {
              query(x) <- group(query(x), grouping(x))
              callNextMethod()
          })

setMethod("tail", "GroupedSolrFrame", function (x, n = 6L, ...) {
              query(x) <- group(query(x), grouping(x))
              callNextMethod()
          })

setGeneric("windows", function(x, ...) standardGeneric("windows"))

setMethod("windows", "GroupedSolrFrame",
          function(x, start = 1L, end = .Machine$integer.max) {
              flip <- start < 0L || end < 0L
              if (flip) {
                  query(x) <- rev(query(x))
                  .start <- start
                  if (end < 0L) start <- abs(end)+1L
                  if (.start < 0L) end <- 0L
              }
              query(x) <- group(query(x), grouping(x),
                                offset=start-1L, limit=end-start+1L)
              df <- as.data.frame(x)
              if (flip) { # FIXME: ideally, reverse prior to split
                  as.data.frame(lapply(df, lapply, rev))
              } else {
                  df
              }
          })

setGeneric("heads", function(x, ...) standardGeneric("heads"))

setMethod("heads", "ANY", function(x, n = 6L) {
              if (!isSingleNumber(n))
                  stop("'n' must be a single, non-NA number")
              windows(x, end = n)
          })

setGeneric("tails", function(x, ...) standardGeneric("tails"))

setMethod("tails", "ANY", function (x, n = 6L) {
              if (!isSingleNumber(n))
                  stop("'n' must be a single, non-NA number")
              windows(x, start = -n + 1L)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setMethod("as.data.frame", "GroupedSolrFrame",
          function(x, row.names = NULL, optional = FALSE, fill = TRUE) {
              fn <- colnames(x)
              if (length(fn) == 0L || grouped(query(x))) {
                  return(as.data.frame(x, row.names=row.names,
                                       optional=optional, fill=fill))
              }
              x <- x[union(fn, all.vars(grouping(x)))]
### FIXME: if we had Solr sort, then CompressedList would be really fast
              df <- as.data.frame(x, row.names=row.names,
                                  optional=optional, fill=fill)
              mf <- model.frame(grouping(x), df)
              ans <- df[fn]
              ans[1L] <- split(ans[[1L]], mf)
              data.frame(ans[1L], lapply(ans[-1L], relist, ans[1L]))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "GroupedSolrFrame", function(object) {
              object <- sort(object, by=grouping(object))
              callNextMethod(as(object, "SolrFrame", strict=TRUE))
              cat("grouping:", grouping(object), "\n")
          })
