### =========================================================================
### FacetList objects
### -------------------------------------------------------------------------
###
### Holds the result of a Solr facet operation
###

### One data.frame of stats for each facet, where the nested facet
### structure is represented by a list. When the user accesses a
### facet, e.g., with a formula or character vector, we can easily
### traverse the list using the base R [[.

setClass("Facets", contains="list",
         representation(stats="data.frame"),
         validity=function(object) {
             validHomogeneousList(object, "Facets")
         })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor (not for the user)
###

Facets <- function(facets, query, schema) {
    if (!is.list(facets)) {
        stop("'facets' must be a list of facet results")
    }
    if (!is.list(query)) {
        stop("'query' must be a list of JSON facet parameters")
    }
    if (!is(schema, "SolrSchema")) {
        stop("'schema' must be a SolrSchema object")
    }
    paths <- enumerateFacetPaths(query)
    stats <- lapply(paths, collapseFacet, facet=facets, query=query)
    subfacets <- list()
    for (i in seq_along(paths)) {
        subfacets[[paths[[i]]]] <- new("Facets", stats=stats[[i]])
    }
    ans <- new("Facets", stats=collapseFacet(facets, query), subfacets)
    postprocessStats(collapseQueryFacets(ans), query, schema)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

stats <- function(x) x@stats

setMethod("[", "Facets",
          function(x, i, j, ..., drop = TRUE) {
              x <- S3Part(x, TRUE)
              new("Facets", callNextMethod())
          })

setMethod("[[", c("Facets", "formula"),
          function(x, i, j, ...) {
              if (!missing(j) || length(list(...)) > 0L) {
                  warning("'j' and arguments in '...' are ignored")
              }
              f <- parseFormulaRHS(i)
              x[[as.character(f)]]
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Munging
###

enumerateFacetPaths <- function(query, pnames=NULL) {
    subfacets <- vapply(query, is.list, logical(1L))
    fnames <- as.list(names(query)[subfacets])
    fnames <- lapply(fnames, function(fn) c(pnames, fn))
    subpaths <- mapply(enumerateFacetPaths, pluck(query[subfacets], "facet"),
                       fnames, SIMPLIFY=FALSE)
    c(fnames, unlist(subpaths, recursive=FALSE))
}

cutLabels <- function(query) {
    levels(cut(query$start, seq(query$start, query$end, query$gap),
               right=grepl("upper", query$include, fixed=TRUE),
               include.lowest=grepl("edge", query$include, fixed=TRUE)))
}

getBucketValues <- function(x, useNA=FALSE) {
    if (length(x) == 0L) {
        factor()
    } else {
        guess <- x[[1L]]$val
        ans <- vpluck(x, "val", guess)
        if (useNA) {
            ans <- c(ans, NA)
        }
        if (is.character(ans)) {
            ans <- factor(ans, exclude=NULL)
        }
        ans
    }
}

getBucketStats <- function(x, proto) {
    mapply(vpluck, names(proto), proto, MoreArgs=list(x=x), SIMPLIFY=FALSE)
}

getProtoStats <- function(exprs) {
    default <- lapply(vapply(exprs, resultWidth, integer(1L)), numeric)
    c(list(count = numeric(1L)), default)
}

## My most complicated recursion ever!
collapseFacet <- function(facet, query, path=character(0L), name=NULL) {
    bucketed <- FALSE
    dropEmptyBuckets <- query$mincount > 0L
    if (is.null(query$type)) {
        val <- NULL
        dropEmptyBuckets <- TRUE
    } else if (query$type == "terms") {
        val <- getBucketValues(facet$buckets, !is.null(facet$missing))
        bucketed <- TRUE
    } else if (query$type == "range") {
        val <- cutLabels(query)
        bucketed <- TRUE
    } else { # facet query
        val <- !grepl("^_not", name)
        name <- sub("^_not_", "", name)
    }
    if (!is.null(query$type)) {
        query <- query$facet
    }
    if (length(path) == 0L) {
        isStat <- !vapply(query, is.list, logical(1L))
        exprs <- query[isStat]
        emptyBucket <- FALSE
        if (bucketed) {
            buckets <- facet$buckets
            if (!is.null(facet$missing) &&
                (!dropEmptyBuckets || facet$missing > 0L)) {
                buckets <- c(buckets, list(facet$missing))
            }
            stats <- getBucketStats(buckets, getProtoStats(exprs))
            nonscalar <- vapply(stats, is.matrix, logical(1L))
        } else {
            emptyBucket <- identical(facet$count, 0)
            if (emptyBucket) {
                stats <- getProtoStats(exprs)
            } else {
                stats <- facet[c("count", names(exprs))]
            }
            nonscalar <- lengths(stats) > 1L
        }
        stats[nonscalar] <- lapply(stats[nonscalar], function(v) I(t(v)))
        stats <- as.data.frame(stats, optional=TRUE)
        if (emptyBucket && dropEmptyBuckets) {
            stats <- stats[FALSE,,drop=FALSE]
        }
        stats$count <- as.integer(stats$count)
        if (is.null(facet)) {
            stats <- data.frame(stats, lapply(exprs, function(x) numeric(0L)))
        }
    } else {
        step <- path[[1L]]
        query <- query[[step]]
        if (bucketed) {
            buckets <- pluck(facet$buckets, step)
            bstats <- lapply(buckets, collapseFacet, query, path[-1L], step)
            stats <- do.call(rbind, bstats)
            if (is.null(stats)) {
                stats <- collapseFacet(NULL, query, path[-1L], step)
            }
            if (length(bstats) > 0L) {
                val <- rep(val, vapply(bstats, nrow, integer(1L)))
            }
        } else {
            stats <- collapseFacet(facet[[step]], query, path[-1L], step)
        }
    }
    if (!is.null(val)) {
        stats <- cbind(rep(val, length=nrow(stats)), stats)
        colnames(stats)[1L] <- name
    }
    stats
}

collapseQueryPair <- function(query, inverted) {
    initialize(query,
               stats=rbind(inverted@stats, query@stats),
               mapply(collapseQueryPair, query, inverted,
                             SIMPLIFY=FALSE))
}

collapseQueryFacets <- function(facets) {
    notnames <- startsWith(names(facets), "_not_")
    qnames <- sub("^_not_", "", names(facets)[notnames])
    facets[qnames] <- mapply(collapseQueryPair,
                             facets[qnames], facets[notnames],
                             SIMPLIFY=FALSE)
    facets[notnames] <- NULL
    initialize(facets, lapply(facets, collapseQueryFacets))
}

### FIXME: at some level it would be nice to cast date statistics back
### to dates (POSIXt), e.g,
## as.POSIXct(x / 1000L, origin="1970-01-01 00:00.00 UTC")
### but it would be tricky to infer the return type.

postprocessStats <- function(facet, query, schema) {
    postprocessStat <- function(stat, expr, addChild = FALSE) {
        if (is.null(expr@postprocess)) {
            return(stat)
        }
        if (!is.null(child(expr))) {
            childName <- hiddenName(child(expr))
            childStat <- postprocessStat(facet@stats[[childName]],
                                         query[[childName]],
                                         addChild=TRUE)
        } else {
            childStat <- facet@stats$count
        }
        ans <- expr@postprocess(stat, childStat)
        if (addChild) {
            attr(ans, "child") <- childStat
        } else {
            attr(ans, "child") <- NULL # perhaps inherited from grandchild
        }
        ans
    }
    exprs <- vapply(query, is, "SolrAggregateCall", FUN.VALUE=logical(1L))
    hidden <- startsWith(names(facet@stats), ".")
    process <- intersect(names(query)[exprs], names(facet@stats)[!hidden])
    facet@stats[process] <- mapply(postprocessStat,
                                   facet@stats[process], query[process],
                                   SIMPLIFY=FALSE)
    facet@stats[] <- lapply(facet@stats, function(x) {
                                if (is(x, "AsIs")) unclass(x) else x
                            })
    factors <- seq_len(match("count", names(facet@stats)) - 1L)
    schema <- augmentToInclude(schema, names(facet@stats)[tail(factors, 1L)])
    facet@stats[factors] <- convertCollection(facet@stats[factors], schema,
                                              fromSolr)
    facet@stats <- facet@stats[!hidden]
    qfacet <- pluck(query[names(facet)], "facet")
    initialize(facet, mapply(postprocessStats, facet, qfacet, SIMPLIFY=FALSE,
                             MoreArgs=list(schema)))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

as.table.Facets <- function(x, ...) as.table(x)

setMethod("as.table", "Facets", function(x) {
              countpos <- which(colnames(x@stats) == "count")
              df <- x@stats[1:countpos]
              count <- df$count
              groupings <- df[-countpos]
              if (length(groupings) == 0L) {
                  stop("no table for root facet")
              }
              dn <- lapply(groupings, function(g) as.character(unique(g)))
              dim <- unname(lengths(dn))
              as.table(array(count, dim=dim, dimnames=dn))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "Facets", function(object) {
              if (length(object) > 0L)
                  cat(labeledLine("subfacets", names(object), sep=", "))
              show(stats(object))
          })
