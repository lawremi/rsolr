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

Facets <- function(facets, query) {
    if (!is.list(facets)) {
        stop("'facets' must be a list of facet results")
    }
    if (!is.list(query)) {
        stop("'query' must be a list of JSON facet parameters")
    }
    paths <- enumerateFacetPaths(query)
    stats <- lapply(paths, collapseFacet, facet=facets, query=query)
    subfacets <- list()
    for (i in seq_along(paths)) {
        subfacets[[paths[[i]]]] <- new("Facets", stats=stats[[i]])
    }
    ans <- new("Facets", stats=collapseFacet(facets, query), subfacets)
    postprocessStats(collapseQueryFacets(ans))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

stats <- function(x) x@stats

setMethod("[", "Facets",
          function(x, i, j, ..., drop = TRUE) {
              new("Facets", callNextMethod())              
          })

setMethod("[[", c("Facets", "formula"),
          function(x, i, j, ...) {
              if (!missing(j) || length(list(...)) > 0L) {
                  warning("'j' and arguments in '...' are ignored")
              }
              f <- parseFormulaRHS(which)
              x[[as.character(f)]]
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Munging
###

enumerateFacetPaths <- function(query, pnames=NULL) {
    subfacets <- vapply(query, is.list, logical(1L))
    fnames <- as.list(names(query)[subfacets])
    fnames <- lapply(fnames, function(fn) c(pnames, fn))
    c(fnames, mapply(enumerateFacetPaths, pluck(query[subfacets], "facet"),
                     fnames, SIMPLIFY=FALSE))
}

cutLabels <- function(query) {
    cut(integer(), seq(query$start, query$end, query$gap),
        right=grepl("upper", query$include, fixed=TRUE),
        include.lowest=grepl("edge", query$include, fixed=TRUE))
}

## My most complicated recursion ever!
collapseFacet <- function(facet, query, path=character(0L), name=NULL) {
    bucketed <- FALSE
    if (is.null(query$type)) {
        val <- NULL
    } else if (query$type == "terms") {
        val <- vpluck(facet$buckets, "val", character(1L))
        bucketed <- TRUE
    } else if (query$type == "range") {
        val <- cutLabels(query)
        bucketed <- TRUE
    } else { # facet query
        val <- !grepl("^_not", name)
        name <- sub("^_not_", name)
    }
    if (length(path) == 0L) {
        snames <- names(query)[!vapply(query, is.list, logical(1L))]
        snames <- c("count", snames) # count is implicit in query
        if (bucketed) {
            stats <- setNames(lapply(snames, pluck, x=facet$buckets), snames)
        } else {
            stats <- facet[snames]
        }
        nonscalar <- lengths(stats) > 1L
        stats[nonscalar] <- lapply(stats[nonscalar], function(v) I(t(v)))
        stats <- as.data.frame(stats)
    } else {
        step <- path[[1L]]
        query <- query[[step]]
        path <- path[-1L]
        if (bucketed) {
            buckets <- pluck(facet$buckets, step)
            bstats <- lapply(buckets, collapseFacet, query$facet, path, step)
            stats <- do.call(rbind, bstats)
            if (length(bstats) > 0L) {
                val <- rep(val, each=nrow(bstats[[1L]]))
            }
        } else {
            stats <- collapseFacet(facet[[step]], query$facet, path, step)
        }
    }
    if (!is.null(val)) {
        stats <- cbind(val, stats)
        colnames(stats)[1L] <- name
    }
    stats
}

collapseQueryPair <- function(query, inverted) {
    query@stats <- rbind(query@stats, inverted@stats)
    query@.Data <- mapply(collapseQueryPair, query, inverted, SIMPLIFY=FALSE)
    query
}

collapseQueryFacets <- function(facet) {
    notnames <- startsWith(names(facet), "_not_")
    qnames <- sub("^_not_", notnames)
    facets[qnames] <- mapply(collapseQueryPair,
                             facets[qnames], facets[notnames],
                             SIMPLIFY=FALSE)
    facets[notnames] <- NULL
    lapply(facets, collapseQueryFacets)
}

postprocessStats <- function(facet, query) {
    exprs <- vapply(query, is, logical(1L), "SolrAggregateExpression")
    pp <- Filter(Negate(is.null), lapply(query[exprs], slot, "postprocess"))
    facet@stats[names(pp)] <- mapply(function(s, pp) pp(s, facet@stats),
                                     facet@stats[names(pp)], pp, SIMPLIFY=FALSE)
    facet@stats[startsWith(names(facet@stats), ".")] <- NULL
    mapply(postprocessStats, facet, pluck(query[names(facet)], "facet"))
}
