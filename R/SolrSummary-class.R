### =========================================================================
### SolrSummary objects
### -------------------------------------------------------------------------
###
### Solr can summarize as tables or sets of statistics. Below, we
### define the data structures for representing Solr summaries.
###

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Classes
###

### Can represent stats on a single field, or conditional every level
### of a field.  A query should generate an array of these objects,
### variable X factor. Could be a 3D array, except there is a variable
### number of levels per column.
setClass("SolrStats",
         representation(min="numeric",
                        max="numeric",
                        sum="numeric",
                        count="numeric", # Solr 'long'
                        missing="numeric", # Solr 'long'
                        sumOfSquares="numeric",
                        mean="numeric",
                        stddev="numeric"))

setClass("SolrStatsList", contains="list",
         validity=function(object) {
           validHomogeneousList(object, "SolrStats")
         })

setClass("SolrStatsFacet",
         representation(levels="character", field="character"),
         contains="SolrStats")

setClass("SolrStatsField",
         representation(facets="SolrStatsList"),
         contains="SolrStats")

setClass("TableList", contains="list",
         validity=function(object) {
           validHomogeneousList(object, "table")
         })

### FIXME: There is an issue here with heterogeneity. Keeping the
### stats and tables in separate components benefits clarity, but
### there is still complexity. Question: should there be separate
### methods on SolrCore, or do we want to compute complex summaries in
### a single call?  SolrQuery supports it, and there is at least one
### use case (a global summary).

### Compare:
### stats(summary(sc, q))$field
### stats(sc, q)$field
### where stats() returns a SolrStatsArray.

### In theory, both could be supported. Or summary could just return a
### result for display, like summary,data.frame. We should probably
### keep summary returning actual values, because it is probably
### common for the user to want to request both tables and stats
### simultaneously.

setClass("SolrSummary",
         representation(stats="SolrStatsList",
                        facets="TableList"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

SolrSummary <- function(x) {
  new("SolrSummary", facets=parseFacetTableList(x),
      stats=parseSolrStatsList(x))
}

SolrStatsField <- function(x) {
  x$facets <- parseSolrStatsFacets(x$facets)
  do.call(new, c("SolrStatsField", x))
}

SolrStatsFacet <- function(x, field) {
  df <- do.call(rbind, lapply(x, function(xi) {
### Currently it does not seem like Solr supports nested facets
    xi$facets <- NULL
    data.frame(xi)
  }))
  do.call(new, c("SolrStatsFacet", field=field,
                 list(levels=fixTrueFalse(names(x))), df))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

facets <- function(x) x@facets
field <- function(x) x@field

setGeneric("stats", function(x, which) standardGeneric("stats"))
setMethod("stats", c("ANY", "missing"), function(x, which) x@stats)
setMethod("stats", c("SolrSummary", "formula"), function(x, which) {
  stats(stats(x), which)
})
setMethod("stats", c("SolrStatsList", "formula"), function(x, which) {
  f <- parseStatsFormula(which)
  ans <- x[f$fields]
  if (!is.null(f$facet)) {
    new("SolrStatsList", lapply(ans, function(ansi) {
      facets(ansi)[[f$facet]]
    }))
  } else {
    ans
  }
})
setMethod("[", c("SolrStatsList", "formula"),
          function(x, i, j, ..., drop = TRUE) {
            if (!missing(j) || length(list(...)) > 0L) {
              warning("'j' and arguments in '...' are ignored")
            }
            if (!isTRUEorFALSE(drop)) {
              stop("'drop' must be TRUE or FALSE")
            }
            ans <- stats(x, i)
            if (length(ans) == 1L && drop) {
              ans <- ans[[1]]
            }
            ans
          })

setMethod("length", "SolrStats", function(x) {
  length(x@min)
})

statTemplate <- function(stat) {
  eval(substitute({
    function(x, na.rm=FALSE) {
      if (!isTRUEorFALSE(na.rm)) {
        stop("'na.rm' must be TRUE or FALSE")
      }
      ans <- x@stat
      if (!na.rm) {
        ans[x@missing > 0] <- NA_real_
      }
      ans
    }
  }))
}

setMethod("min", "SolrStats", statTemplate(min))
setMethod("max", "SolrStats", statTemplate(max))
setMethod("sum", "SolrStats", statTemplate(sum))
mean.SolrStats <- statTemplate(mean)
setMethod("mean", "SolrStats", mean.SolrStats)
setMethod("sd", "SolrStats", statTemplate(stddev))
setMethod("var", "SolrStats", function(x, y=NULL, na.rm=FALSE, use) {
  if (!missing(y) || !missing(na.rm) || !missing(use)) {
    warning("args 'y', 'na.rm', and 'use' are ignored")
  }
  x@stddev^2
})
setGeneric("count", function(x, ..., na.rm=FALSE) standardGeneric("count"))
setMethod("count", "SolrStats", function(x, na.rm=FALSE) {
  if (!isTRUEorFALSE(na.rm)) {
    stop("'na.rm' must be TRUE or FALSE")
  }
  if (na.rm) {
    x@count
  } else {
    x@count + x@missing
  }
})

levels.SolrStatsFacet <- function(x) x@levels
setMethod("levels", "SolrStatsFacet", levels.SolrStatsFacet)

setMethod("[", "TableList",
          function(x, i, j, ..., drop = TRUE) {
            new("TableList", callNextMethod())
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Parsing
###

### FIXME: probably want to identify boolean values from the schema
fixTrueFalse <- function(x) {
  if (!is.null(x) && setequal(x[!is.na(x)], c("false", "true"))) {
    toupper(x)
  } else x
}

parseFacetTableList <- function(x) {
  new("TableList", c(parseFacetFieldTables(x), parseFacetQueryTables(x),
                     parseFacetRangeTables(x), parseFacetPivotTables(x)))
}

parseFacetQueryTables <- function(x) {
  query <- query(origin(x))
  facet.query <- query@params[names(query@params) == "facet.query"]
  counts <- relist(unlist(x$facet_counts$facet_queries),
                   as.list(unlist(unname(facet.query), recursive=FALSE)))
  ans <- mapply(function(xi, nm) {
    mode(xi) <- "integer"
    if (length(xi) > 1L) {
      tab <- as.table(xi)
    } else {
      total <- as.integer(x$response$numFound)
      tab <- as.table(c("FALSE"=total-xi, "TRUE"=xi))
    }
    names(dimnames(tab)) <- nm
    tab
  }, counts, names(counts), SIMPLIFY=FALSE)
  setNames(ans, names(counts))
}

getFacetQueryGroup <- function(x) {
  val <- attr(x, "group")
  if (!is.null(val)) val else NA_character_
}

parseFacetFieldTables <- function(x) {
  facet.fields <- x$facet_counts$facet_fields
  mapply(function(xi, nm) {
    mode(xi) <- "integer"
    ##xi <- head(xi, -1L)
    names(xi) <- fixTrueFalse(names(xi))
    tab <- as.table(xi)
    names(dimnames(tab)) <- nm
    tab
  }, facet.fields, names(facet.fields), SIMPLIFY=FALSE)
}

parseFacetRangeTables <- function(x) {
  counts <- x$facet_counts$facet_ranges
  mapply(function(xi, nm) {
    include <-
      x$responseHeader$params[[paste0("f.", nm, ".facet.range.include")]]
    labels <- levels(cut(integer(), seq(xi$start, xi$end, xi$gap),
                         right = include == "upper"))
    tab <- as.table(setNames(as.integer(xi$counts), labels))
    names(dimnames(tab)) <- nm
    tab
  }, counts, names(counts), SIMPLIFY=FALSE)
}

## If a single field remaining, parse into a DF,
## Otherwise, if there is a pivot element, recurse into it,
## Otherwise, loop over the values, recursing on each.

## depends on facet.pivot.mincount=0
parsePivot <- function(pivot, fields) {
  if (length(fields) == 1L) {
    df <- restfulr:::raggedListToDF(pivot)
    df$field <- NULL
    df$value <- as.factor(df$value)
  } else if (!is.null(pivot$pivot)) {
    df <- parsePivot(pivot$pivot, fields[-1L])
    value <- if (is.null(pivot$value)) NA else pivot$value
    df <- cbind(as.factor(value), df)
  } else {
    df <- do.call(rbind, lapply(pivot, parsePivot, fields))
  }
  colnames(df)[1L] <- fields[1L]
  df
}

parseFacetPivotTables <- function(x) {
  fields <- strsplit(as.character(names(x$facet_counts$facet_pivot)), ",",
                     fixed=TRUE)
  mapply(function(pivot, fields) {
    dfToTable(parsePivot(pivot, fields))
  }, x$facet_counts$facet_pivot, fields, SIMPLIFY=FALSE)
}

parseSolrStatsList <- function(x) {
  new("SolrStatsList", lapply(x$stats$stats_fields, SolrStatsField))
}

parseSolrStatsFacets <- function(x) {
  new("SolrStatsList", mapply(SolrStatsFacet, x, names(x), SIMPLIFY=FALSE))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

as.data.frame.SolrStats <- function(x, row.names = NULL, optional = FALSE, ...) {
  slots <- slotsAsList(x)
  slots$facets <- NULL
  as.data.frame(slots, row.names=row.names, optional=optional, ...)
}
setMethod("as.data.frame", "SolrStats", as.data.frame.SolrStats)

as.data.frame.SolrStatsFacet <- function(x, row.names = NULL, optional = FALSE,
                                         ...)
{
  df <- NextMethod()
  rownames(df) <- levels(x)
  colnames(df)[1] <- field(x)
  df$field <- NULL
  df
}
setMethod("as.data.frame", "SolrStatsFacet", base::as.data.frame)

as.data.frame.SolrStatsList <- function(x, row.names = NULL, optional = FALSE,
                                        ...)
{
  dummyForEmptyCase <- new("SolrStats")
  data.frame(field=rep(as.character(names(x)), elementLengths(x)),
             do.call(rbind, lapply(c(dummyForEmptyCase, x), as.data.frame,
                                   row.names=row.names, optional=optional)),
             stringsAsFactors=FALSE, ...)
}
setMethod("as.data.frame", "SolrStatsList", as.data.frame.SolrStatsList)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrStats", function(object) {
  show(as.data.frame(object))
})

setMethod("show", "SolrStatsList", function(object) {
  show(as.data.frame(object))
})

setMethod("show", "SolrStatsField", function(object) {
  callNextMethod()
  cat(BiocGenerics:::labeledLine("facets", names(facets(object))), "\n")
})

setMethod("show", "TableList", function(object) {
  nlevels <- 6L
  if (length(object) == 0L) {
    cat("< empty list of tables >\n")
  } else {
    nlevels <- min(nlevels, max(elementLengths(object)))
    mat <- do.call(cbind, lapply(object, formatTable, nlevels=nlevels))
    tab <- as.table(mat)
    rownames(tab) <- rep("", nrow(tab))
    show(tab)
  }
})

setMethod("show", "SolrSummary", function(object) {
  cat("facets (with at least one level):\n")
  show(facets(object)[elementLengths(facets(object)) > 0L])
  cat("\nstats:\n")
  show(stats(object))
})
