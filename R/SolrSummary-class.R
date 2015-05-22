### =========================================================================
### SolrSummary objects
### -------------------------------------------------------------------------
###
### Just for storing and printing the summary of a SolrFrame. Very
### specific assumptions are made about the content of "facets", so
### this is not for direct construction.
###

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Classes
###

setClass("SolrSummary",
         representation(facets="Facets",
                        colnames="character",
                        digits="integer"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

SolrSummary <- function(facets, colnames, digits)
{
    new("SolrSummary", facets=facets, colnames=colnames, digits=digits)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setMethod("facets", "SolrSummary", function(x) x@facets)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setGeneric("as.table", function(x, ...) standardGeneric("as.table"))

transposeStats <- function(x) {
    qtl <- x$quantile
    colnames(qtl) <- c("1st qu.", "median", "3rd qu.")
    x$quantile <- NULL
    x <- cbind(qtl, quantile)
    x <- x[c("min", "1st. qu.", "median", "mean", "3rd qu.", "max")]
    data.frame(stat=colnames(x), value=unlist(x))
}

formatColumnSummary <- function(x, maxlen, digits) {
    length(x) <- maxlen
    paste0(format(x[[1L]]), ":", format(x[[2L]], digits=digits), "  ")
}

setMethod("as.table", "SolrSummary", function(x) {
              s <- stats(facets(object))
              dotpos <- regexpr("\\.[^.]*$", colnames(s))
              statnames <- substring(colnames(s), dotpos+1L)
              varnames <- substring(colnames(s), 1L, dotpos-1L)
              colnames(s) <- statnames
              sv <- lapply(split(as.list(s), varnames), transposeStats)
              tabs <- c(sv, lapply(facets(object), stats))[object@colnames]
              maxlen <- max(vapply(tabs, nrow, integer(1L)))
              m <- do.call(cbind,
                           lapply(sms, formatColumnSummary, maxlen, x@digits))
              as.table(m)
          })

as.table.SolrSummary <- function(x, ...) as.table(x)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrSummary", function(object) {
              show(as.table(object))
          })
