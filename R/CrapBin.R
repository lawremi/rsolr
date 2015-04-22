setMethod("fundamentalOverride", c("Expression", "language"), function(x, fun) {
              RestrainedForceGeneric(x, baseenv())
          })
all.funs <- function(expr) {
    nms <- all.names(expr)
    vars <- all.vars(expr, unique=FALSE)
    levs <- unique(nms)
    tab <- table(factor(nms, levs)) - table(factor(vars, levs))
    levs[tab > 0]
}

FunsEnv <- function(x, expr, parent) {
    funs <- all.funs(expr)
    lst <- list()
    fundamentals <- c("{", "if", "<-", "for", "while", "repeat", "(")
    lst[fundamentals] <- lapply(fundamentals,
                                function(f) fundamentalOverride(x, new(f)))
    l$"-" <- minus
    others <- setdiff(funs, names(lst))
    lst[others] <- lapply(others, RestrainedForceGeneric, env=parent)
    overrides <- overrides(x)
    lst[names(overrides)] <- overrides
    list2env(lst, parent=parent)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Partial laziness
###
### The translation environment is populated with a wrapper for each
### function name. The wrapper determines whether a function has
### been declared (as an S4 method) to accept expressions, or whether
### we need to force evaluation and materialize the data in R.
###
### Figuring out which arguments to force is a tough problem. The
### simplest approach would be to simply force ALL expressions. This
### would work for all non-generics and single dispatch generics, as
### well as all generics where there are no lazy methods. For the case
### where only some of the signature arguments are lazy (like one out
### of two) we need to find that method and force arguments to match
### that method. Finding that method is tough, and there might be
### multiple top methods. Another important consideration is the cost
### of forcing. Optimizing for all of these concerns would be
### complicated. Thus, we define a forceBefore() generic that lets us
### optimize by the expression itself, and hope for the best.
###
###
###

setGeneric("forceBefore", function(x, y) standardGeneric("forceBefore"))

setMethod("forceBefore", c("ANY", "ANY"),
          function(x, y) FALSE)

firstToForce <- function(args) {
    Reduce(function(i, j) {
               if (forceBefore(args[[i]], args[[j]])) i else j
           }, seq_along(args))
}

signatureForArgs <- function(fun, args) {
    sigArgs <- match(getGeneric(fun)@signature, names(args))
    sig <- vapply(args[sigArgs], function(x) class(x)[1L], character(1L))
    sig[is.na(sigArgs)] <- "missing"
    sig
}

hasLazyMethod <- function(fun, args, isLazy) {
    if (!isGeneric(fun)) {
        return(FALSE)
    }
    callSig <- signatureForArgs(fun, args)
    methodSig <- selectMethod(fun, classes)@defined
    all(vapply(methodSig[isLazy[sigArgs]], extends, "Expression",
               FUN.VALUE=logical(1L)))
}

RestrainedForceGeneric <- function(name, env) {
    eval(substitute({
        FUN <- get(name, env, mode="function")
        function(...) {
            args <- list(...)
            mc <- match.call()
            namedArgs <- setNames(args, names(mc[-1L]))
            isLazy <- vapply(args, is, "Expression", FUN.VALUE=logical(1L))
            if (any(isLazy) && !hasLazyMethod(FUN, namedArgs, isLazy))
                {
                    force <- firstToForce(args[isLazy])
                    args[isLazy][[force]] <- eval(args[isLazy][[force]], env)
                }
            do.call(FUN, args)
        }
    },  list(FUN=as.name(name))))
}

### An alternative to this approach is to expect the user to
### explicitly materialize when necessary. The API for that is not
### clear. We could convert force() into a generic. It is unfortunate
### that we cannot use as.vector, as.character, etc, since those
### operate on the expression itself.

### Better solution: Introduce concept of Reference, which is composed
### of an Expression. All translation methods will have a
### corresponding Reference-based wrapper, i.e., there is a Reference
### in the signature for each Expression. The Reference could also
### point to the data source (like Solr). It would then be dropped
### from the Expression subclass.

### To handle that change, SolrQuery should probably gain a SolrCore
### slot, which is optionally NULL. SolrQuery will combine itself with
### the SolrCore into a Solr(List) object, which is then passed down
### to translate() via a third 'context' argument. That 'context' is
### dispatched on when creating Reference objects.

### Another danger: while dispatch works across function call
### boundaries, some functions, like '(' will not. No way of fixing
### that without changing R itself.
