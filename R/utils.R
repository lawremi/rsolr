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
