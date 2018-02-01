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
### Eventually, we might want special grouped Promise subclasses.
### But for now we can get away with basic promises.

### FIXME: Need some way to transform based on grouped stats. Like:

## > transform(x, x.norm=x/mean(x))

### That works "fine" (via multiple requests) for ungrouped data, but
### generating a separate summary for each group would just not
### scale. Solr (and thus us) could support this by introducing a
### stat() function that refers to a statistic in the facets. That is
### a pretty high bar though.

### The closest we have right now requires modification of the Solr core:

## gx <- group(x, ~ group)
## gx[,"foo.mean_d"] <- mean(gx$foo)
## transform(x, foo.norm=foo/foo.mean_d)

### But an atomic update at least requires transmitting the IDs

### Of course, we could always just force...

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

setMethod("grouping", "GroupedSolrFrame", function(x) x@grouping)

setMethod("ndoc", "GroupedSolrFrame", function(x) {
              as.integer(xtabs(NULL, x))
          })

setMethod("rownames", "GroupedSolrFrame", function(x) {
              x <- x[all.vars(grouping(x))]
              groups <- unique(as(x, "SolrFrame", strict=TRUE))
              do.call(paste, c(groups, sep="."))
          })

setMethod("nrow", "GroupedSolrFrame", function(x) {
              length(ndoc(x))
          })

setMethod("ngroup", "GroupedSolrFrame", function(x) {
              nrow(x)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### CREATE/UPDATE/DELETE
###

setReplaceMethod("[", "GroupedSolrFrame", function(x, i, j, ..., value) {
                     normValue <- function(v, nd) {
                         if (all(lengths(v) == 1L)) {
                             rep(unlist(v, use.names=FALSE), nd)
                         } else {
                             needrep <- lengths(v) != nd
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
                         } else if (!is.list(i) && !is(i, "Promise")) {
                             i <- ids(x)[i]
                         }
                         i <- ungroup(i)
                     }
                     group(callNextMethod(), grouping(x))
                 })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### READ
###

setMethod("[", "GroupedSolrFrame", function(x, i, j, ..., drop = TRUE) {
              twoD <- (nargs() - !missing(drop)) > 2L
              if (twoD && !missing(i) &&
                  !(is(i, "Promise") || is(i, "Expression"))) {
                  if (is.list(i) && !is.null(names(i)) &&
                      !all(vapply(i, is.character, logical(1L)))) {
                      x <- x[names(i),]
                  } else if (!is.list(i)) {
                      i <- ids(x)[i]
                  }
                  i <- ungroup(i)
              }
              if (twoD && missing(i) && !missing(j)) {
                  ## FIXME: methods package bug
                  callNextMethod(x, i=, j=j, drop=drop)
              } else {
                  callNextMethod()
              }
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Summarizing
###
### Any specified groupings are interacted with the current grouping.
###

setMethod("group", "GroupedSolrFrame", function(x, by) {
              if (is.null(by)) {
                  return(x)
              }
              if (!is(by, "formula")) {
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

setMethod("unique", "GroupedSolrFrame", function (x, incomparables = FALSE) {
              ans <- callNextMethod()
              groupDf(ans, grouping(x), colnames(x))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

groupDf <- function(df, grouping, fn) {
    df <- do.call(data.frame, df)
    mf <- model.frame(grouping, df)
    df <- df[fn]
    columns <- lapply(df, function(x) unname(split(x, mf)))
    ## firstColumn <- unname(split(df[[1L]], mf))
    ## columns <- c(list(firstColumn), lapply(df[-1L], relist, firstColumn))
    ## names(columns)[1L] <- fn[1L]
    structure(columns, class="data.frame", row.names=seq_along(columns[[1L]]))
}

setMethod("as.data.frame", "GroupedSolrFrame",
          function(x, row.names = NULL, optional = FALSE, fill = TRUE) {
              fn <- colnames(x)
              if (length(fn) == 0L || grouped(query(x))) {
                  return(callNextMethod())
              }
              x <- x[union(fn, all.vars(grouping(x)))]
### FIXME: if we had Solr sort, then CompressedList would be really fast
              df <- callNextMethod()
              groupDf(df, grouping(x), fn)
          })

setGeneric("ungroup", function(x, ...) standardGeneric("ungroup"))

setMethod("ungroup", "data.frame", function(x) {
              x[] <- lapply(x, unlist, use.names=FALSE)
              x
          })

setMethod("ungroup", "ANY", function(x) {
              unlist(x, recursive=FALSE, use.names=FALSE)
          })

setMethod("ungroup", "GroupedSolrFrame", function(x) {
              as(x, "SolrFrame")
          })

setMethod("ungroup", "SolrFrame", function(x) {
              x
          })

setMethod("unlist", "SolrPromise",
          function(x, recursive = TRUE, use.names = TRUE) {
              context(x) <- ungroup(context(x))
              x
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "GroupedSolrFrame", function(object) {
              object <- sort(object, by=grouping(object))
              callNextMethod(as(object, "SolrFrame", strict=TRUE))
              cat("grouping:", grouping(object), "\n")
          })
