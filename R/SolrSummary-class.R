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

transposeStats <- function(x) {
    qtl <- as.vector(x$quantile)
    names(qtl) <- c("1st qu.", "median", "3rd qu.")
    x$quantile <- NULL
    x <- c(x, qtl)
    x <- x[c("min", "1st qu.", "median", "mean", "3rd qu.", "max", "NA's")]
    data.frame(stat=names(x), value=unlist(x, use.names=FALSE))
}

formatColumnSummary <- function(x, maxlen, digits) {
    ans <- paste0(format(x[[1L]]), ":", format(x[[2L]], digits=digits), "  ")
    c(ans, rep("", maxlen - length(ans)))
}

setMethod("as.table", "SolrSummary", function(x) {
              s <- stats(facets(x))
              s$count <- NULL
              dotpos <- regexpr("\\.[^.]*$", colnames(s))
              statnames <- substring(colnames(s), dotpos+1L)
              varnames <- substring(colnames(s), 1L, dotpos-1L)
              colnames(s) <- statnames
              sv <- lapply(split(as.list(s), varnames), transposeStats)
              tabs <- c(sv, lapply(facets(x), stats))[x@colnames]
              maxlen <- max(vapply(tabs, nrow, integer(1L)))
              m <- do.call(cbind,
                           lapply(tabs, formatColumnSummary, maxlen, x@digits))
              as.table(m)
          })

as.table.SolrSummary <- function(x, ...) as.table(x)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrSummary", function(object) {
              show(as.table(object))
          })
