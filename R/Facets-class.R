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
    postprocessStats(collapseQueryFacets(ans), query)
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
    subpaths <- mapply(enumerateFacetPaths, pluck(query[subfacets], "facet"),
                       fnames, SIMPLIFY=FALSE)
    c(fnames, unlist(subpaths, recursive=FALSE))
}

cutLabels <- function(query) {
    levels(cut(integer(), seq(query$start, query$end, query$gap),
               right=grepl("upper", query$include, fixed=TRUE),
               include.lowest=grepl("edge", query$include, fixed=TRUE)))
}

getBucketValues <- function(x) {
    if (length(x) == 0L) {
        factor()
    } else {
        guess <- x[[1L]]$val
        vpluck(x, "val", guess)
    }
}

getBucketStats <- function(x, names) {
    if (length(x) == 0L) {
        svalue <- list(numeric())
    } else {
        svalue <- x[[1L]][names]
    }
    mapply(vpluck, names, svalue, MoreArgs=list(x=x), SIMPLIFY=FALSE)
}

## My most complicated recursion ever!
collapseFacet <- function(facet, query, path=character(0L), name=NULL) {
    bucketed <- FALSE
    if (is.null(query$type)) {
        val <- NULL
    } else if (query$type == "terms") {
        val <- getBucketValues(facet$buckets)
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
        snames <- names(query)[!vapply(query, is.list, logical(1L))]
        snames <- c("count", snames) # count is implicit in query
        if (bucketed) {
            stats <- getBucketStats(facet$buckets, snames)
            nonscalar <- vapply(stats, is.matrix, logical(1L))
        } else {
            stats <- facet[snames]
            nonscalar <- lengths(stats) > 1L
        }
        stats[nonscalar] <- lapply(stats[nonscalar], function(v) I(t(v)))
        stats <- as.data.frame(stats)
### FIXME: ensure that calls to Solr unique() yield integer
        stats$count <- as.integer(stats$count)
    } else {
        step <- path[[1L]]
        query <- query[[step]]
        path <- path[-1L]
        if (bucketed) {
            buckets <- pluck(facet$buckets, step)
            bstats <- lapply(buckets, collapseFacet, query, path, step)
            stats <- do.call(rbind, bstats)
            if (length(bstats) > 0L) {
                val <- rep(val, each=nrow(bstats[[1L]]))
            }
        } else {
            stats <- collapseFacet(facet[[step]], query, path, step)
        }
    }
    if (!is.null(val)) {
        stats <- cbind(val, stats)
        if (length(path) > 1L) {
            perm <- c(seq_along(path)[-1L], 1L)
            dim <- vapply(stats[perm], nlevels, integer(1L))
            stats <- stats[aperm(array(seq_len(nrow(stats)), dim), perm),]
        }
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

postprocessStats <- function(facet, query) {
    exprs <- vapply(query, is, "SolrAggregateCall", FUN.VALUE=logical(1L))
    pp <- Filter(Negate(is.null), lapply(query[exprs], slot, "postprocess"))
    facet@stats[names(pp)] <- mapply(function(s, pp) pp(s, facet@stats),
                                     facet@stats[names(pp)], pp, SIMPLIFY=FALSE)
    hidden <- startsWith(names(facet@stats), ".")
    facet@stats <- facet@stats[!hidden]
    qfacet <- pluck(query[names(facet)], "facet")
    initialize(facet, mapply(postprocessStats, facet, qfacet, SIMPLIFY=FALSE))
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
              dn <- lapply(groupings, function(g) as.character(unique(g)))
              dim <- unname(lengths(dn))
              as.table(array(count, dim=dim, dimnames=dn))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "Facets", function(object) {
              if (length(object) > 0L)
                  cat(BiocGenerics:::labeledLine("subfacets", names(object),
                                                 sep=", "))
              show(stats(object))
          })
