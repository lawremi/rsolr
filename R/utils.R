### =========================================================================
### Utilities
### -------------------------------------------------------------------------

csv <- function(x) {
  paste(x, collapse = ",")
}

pluck <- function(x, name, default=NULL) {
  ans <- lapply(x, `[[`, name)
  if (!is.null(default)) {
    ans[vapply(ans, is.null, logical(1))] <- list(default)
  }
  ans
}

vpluck <- function(x, name, value=default, default=NULL) {  
  ans <- vapply(x, `[[`, value, name)
  if (!is.null(default)) {
    ans[vapply(ans, is.null, logical(1))] <- list(default)
  }
  ans
}

isSingleString <- function(x) {
  is.character(x) && length(x) == 1L && !is.na(x)
}

isTRUEorFALSE <- function(x) {
  identical(x, TRUE) || identical(x, FALSE)
}

recycleVector <- function(x, length.out)
{
  if (length(x) == length.out) {
    x
  } else {
    ans <- vector(storage.mode(x), length.out)
    ans[] <- x
    ans
  }
}

## uses c() to combine high-level classes like Date
simplify2array2 <- function(x) {
  uniq.lengths <- unique(vapply(x, length, integer(1)))
  if (length(uniq.lengths) != 1L) {
    x
  } else if (uniq.lengths == 1L) {
    do.call(c, x)
  } else {
    simplify2array(x, higher=FALSE)
  }
}

elementLengths <- function(x) {
  vapply(x, length, logical(1))
}

truncateTable <- function(x, nlevels) {
  if (length(x) > nlevels) {
    x <- c(head(x, nlevels-1L),
                    "<other>"=sum(tail(x, -(nlevels-1L))))
  }
  x
}

formatTable <- function(x, nlevels) {
  sorted.tab <- truncateTable(sort(x, decreasing=TRUE), nlevels)
  c(paste0(names(sorted.tab), ": ", sorted.tab),
    rep("", nlevels-length(sorted.tab)))
}

dfToTable <- function(x) {
  factors <- x[-length(x)]
  level.lens <- vapply(factors, function(xi) {
    length(levels(xi))
  }, integer(1))
  tab <- as.table(array(0L, level.lens))
  tab[as.matrix(factors)] <- x$count
  tab
}

validHomogeneousList <- function(x, type) {
  if (!all(as.logical(lapply(x, is, type))))
    paste("all elements of", class(x), "must be", type, "objects")
}

top_prenv <- function(x) {
  .Call2("top_prenv", substitute(x), parent.frame(), PACKAGE="rsolr")
}
