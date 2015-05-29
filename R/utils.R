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

vpluck <- function(x, name, value, required=TRUE) {
  if (required) {
    vapply(x, `[[`, value, name)
  } else {
    as.vector(pluck(x, name, value), mode=mode(value))
  }
}

isSingleString <- function(x) {
  is.character(x) && length(x) == 1L && !is.na(x)
}

isSingleNumber <- function(x) {
  is.numeric(x) && length(x) == 1L && !is.na(x)
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
  x[vapply(x, is.null, logical(1L))] <- NA # somewhat debatable
  uniq.lengths <- unique(lengths(x))
  if (length(uniq.lengths) != 1L) {
    x
  } else if (uniq.lengths == 1L) {
    do.call(c, x)
  } else {
    simplify2array(x, higher=FALSE)
  }
}

grouping <- function(x) {
  rep(seq_along(x), lengths(x))
}

truncateTable <- function(x, nlevels) {
  names(x) <- do.call(paste, c(expand.grid(dimnames(x)), sep=","))
  if (length(x) > nlevels) {
    x <- c(head(sort(x, decreasing=TRUE), nlevels-1L),
                    "<other>"=sum(tail(x, -(nlevels-1L))))
  }
  x
}

formatTable <- function(x, nlevels) {
  trunc.tab <- truncateTable(x, nlevels)
  c(paste0(names(trunc.tab), rep(": ", length(trunc.tab)), trunc.tab),
    rep("", nlevels-length(trunc.tab)))
}

dfToTable <- function(x) {
  factors <- x[-length(x)]
  levels <- lapply(factors, levels)
  tab <- as.table(array(0L, unname(lengths(levels))))
  dimnames(tab) <- levels
  tab[as.matrix(factors)] <- as.integer(x$count)
  tab
}

validHomogeneousList <- function(x, type) {
  if (!all(vapply(x, is, type, FUN.VALUE=logical(1L))))
    paste("all elements of", class(x), "must be", type, "objects")
}

validHomogeneousTree <- function(x, type) {
    if (!all(vapply(x, function(xi) {
                        if (is.list(xi))
                            unlist(validHomogeneousTree(xi, type))
                        else is(xi, type)
                    }, FUN.VALUE=logical(1L))))
        paste("all leaf elements of", class(x), "must be", type, "objects")
}

top_prenv <- function(x) {
  sym <- substitute(x)
  if (!is.name(sym)) {
    stop("'x' did not substitute to a symbol")
  }
  .Call("top_prenv", sym, parent.frame(), PACKAGE="rsolr")
}

top_prenv_dots <- function(...) {
  .Call("top_prenv_dots", environment(), PACKAGE="rsolr")
}

slotsAsList <- function(x) {
  sapply(slotNames(x), slot, object=x, simplify=FALSE)
}

signatureClasses <- function(fun, pos) {
  matrix(unlist(findMethods(fun)@signatures), length(fun@signature))[pos,]
}

slotLengths <- function(x) {
  vapply(slotNames(x), function(s) length(slot(x, s)), integer(1))
}

slotHasNAs <- function(x) {
  vapply(slotNames(x), function(s) any(is.na(slot(x, s))), logical(1))
}

normColIndex <- function(x, f) {
  if (is.character(f))
    f <- colnames(x) %in% f
  else if (is.numeric(f)) {
    tmp <- logical(ncol(x))
    tmp[f] <- TRUE
    f <- tmp
  } else if (!is.logical(f)) {
    stop("'", deparse(substitute(f)),
         "' must be character, numeric or logical")
  }
  f
}

ROWNAMES <- function (x) if (length(dim(x)) != 0L) rownames(x) else names(x)

stripI <- function(x) {
    if (is.call(x) && x[[1L]] == quote(I))
        x[[2L]]
    else x
}

setMethods <- function (f, signatures = list(), definition,
                        where = topenv(parent.frame()), ...) 
{
    for (signature in signatures)
        setMethod(f, signature = signature, definition, where = where, ...)
}

wrapParens <- function(x) paste0("(", x, ")")

VariadicToBinary <- function(variadic, binary)
{
    args <- formals(variadic)
    args$... <- NULL
    forwardArgs <- setNames(lapply(names(args), as.name), names(args))
    reduceBody <- as.call(c(list(binary, quote(..x), quote(..y)),
                            forwardArgs))
    reduceFun <- substitute(function(..x, ..y) BODY, list(BODY=reduceBody))
    body(variadic) <- substitute(Reduce(FUN2, list(...)), list(FUN2=reduceFun))
    variadic
}

callsToNames <- function(expr, fun, where = parent.frame()) {
    unquote <- function(e) if (is.pairlist(e)) 
                               as.pairlist(lapply(e, unquote))
                           else if (length(e) <= 1L) 
                               e
                           else if (e[[1L]] == fun)
                               as.name(eval(e[[2L]], where))
                           else as.call(lapply(e, unquote))
    unquote(expr)
}

list2LazyEnv <- function(x, enclos) {
    env <- new.env(parent=enclos)
    for (nm in names(x)) {
        makeActiveBinding(nm, function(v) {
                              if (missing(v)) {
                                  v <- x[[nm]]
                                  rm(names=nm, env)
                              }
                              assign(nm, v, env)
                              v
                          })
    }
}

varsEnv <- function(expr, frame, parent) {
    vars <- all.vars(expr)
    objs <- lapply(vars, function(i) frame[[i]]) # could support mget()...
    list2env(setNames(objs, vars), parent=parent)
}

startsWith <- function(x, prefix) {
    substring(x, 1L, nchar(prefix)) == prefix
}

parseFormulaRHS <- function(x) {
    if (length(x) != 2L) {
        stop("formula must not have an LHS")
    }
    lapply(attr(terms(x), "variables")[-1L], stripI)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Pretty printing stolen^B^B adapted from S4Vectors
###

### showHeadLines and showTailLines robust to NA, Inf and non-integer 
.get_showLines <- function(default, option)
{
  opt <- getOption(option, default=default)
  if (!is.infinite(opt))
    opt <- as.integer(opt)
  if (is.na(opt))
    opt <- default
  opt 
}

get_showHeadLines <- function()
{
  .get_showLines(5L, "showHeadLines") 
}

get_showTailLines <- function()
{
  .get_showLines(5L, "showTailLines") 
}

prettyRownames <- function(x, len=NULL, nhead=NULL, ntail=NULL)
{
  names <- ROWNAMES(x)
  if (is.null(nhead) && is.null(ntail)) {
    ## all lines
    if (len == 0L)
      character(0)
    else if (is.null(names))
      paste0("[", seq_len(len), "]")
    else
      names
  } else {
    ## head and tail
    if (!is.null(names)) {
      c(head(names, nhead), "...", tail(names, ntail))
    } else {
      if (nhead == 0L)
        s1 <- character(0)
      else s1 <- paste0("[", seq_len(nhead), "]")
      if (ntail == 0L)
        s2 <- character(0)
      else s2 <- paste0("[", (len - ntail) + seq_len(ntail), "]")
      c(s1, "...", s2)
    }
  }
}

makeNakedMat_default <- function(x) {
  m <- as.matrix(data.frame(x))
  mode(m) <- "character"
  m
}

### 'makeNakedMat.FUN' must be a function returning a character matrix.
makePrettyMatrixForCompactPrinting <-
  function(x, makeNakedMat.FUN=makeNakedMat_default)
{
  lx <- NROW(x)
  nhead <- get_showHeadLines()
  ntail <- get_showTailLines()

  if (lx < (nhead + ntail + 1L)) {
    ans <- makeNakedMat.FUN(x)
    ans_rownames <- prettyRownames(x, lx)
  } else {
    ans_top <- makeNakedMat.FUN(head(x, nhead))
    ans_bottom <- makeNakedMat.FUN(tail(x, ntail))
    ans <- rbind(ans_top,
                 matrix(rep.int("...", ncol(ans_top)), nrow=1L),
                 ans_bottom)
    ans_rownames <- prettyRownames(x, lx, nhead, ntail)
  }
  rownames(ans) <- format(ans_rownames, justify="right")
  ans
}

