### =========================================================================
### Expression objects
### -------------------------------------------------------------------------
###
### Represents a target of the translate() generic.
###

setClass("Expression")

setClass("SimpleExpression",
         representation(expr="character"),
         prototype(expr=""),
         contains="Expression",
         validity=function(object) {
           if (!isSingleString(object@expr)) {
             stop("'expr' must be a single, non-NA string")
           }
         })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Symbols
###
### A Symbol is an R name/symbol that drives translation of an R
### expression through dispatch on the Symbol (sub)class. Translation
### occurs by evaluating the R expression in an environment where all
### of the names resolve to Symbol objects.
###

setClass("Symbol", contains=c("name", "Expression"))

### Factory function for Symbol objects (by target Expressions)
setGeneric("Symbol", function(x, name) standardGeneric("Symbol"))

setMethod("Symbol", c("ANY", "character"), function(x, name) {
              Symbol(x, as.name(name))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Environment construction
###
### We rely on dispatch (i.e., the strategy pattern), to construct an
### environment for each expression type. Evaluating the R expression
### in the environment should result in the desired expression object.
###
### Drawbacks:
### - No way to dispatch to zero-arg function calls,
###   or calls with only literal arguments,
### - Some functions do not have generics, or they do not have the
###   necessary arguments in their generic signature.
###
### Thus, we need a way for the target expression to augment the
### environment to handle special cases.
###

VarsEnv <- function(x, expr, parent) {
    vars <- all.vars(expr)
    syms <- setNames(lapply(vars, Symbol), vars)
    list2env(syms, parent=parent)
}

setGeneric("fundamentalOverride",
           function(x, fun) standardGeneric("fundamentalOverride"))

setMethod("fundamentalOverride", c("Expression", "language"), function(x, fun) {
              eval(fun[[1L]])
          })

setMethod("fundamentalOverride", c("SolrLuceneExpression", "("),
          function(x, fun) {
              function(x) {
                  if (is(x, "SolrLuceneExpression")) {
                      LuceneExpression(wrapParens(x))
                  } else {
                      x
                  }
              }
          })

FunsEnv <- function(x, expr, parent) {
    lst <- list()
    fundamentals <- c("{", "if", "<-", "for", "while", "repeat", "(")
    lst[fundamentals] <- lapply(fundamentals,
                                function(f) fundamentalOverride(x, new(f)))
    l$"-" <- minus
    overrides <- overrides(x)
    lst[names(overrides)] <- overrides
    list2env(lst, parent=parent)
}

setMethod("translationContext", "Expression",
          function(x, expr, parent = emptyenv()) {
              VarsEnv(x, expr, FunsEnv(x, expr, parent))
          })

setGeneric("overrides", function(x) standardGeneric("overrides"))

setMethod("overrides", "SolrExpression", function(x) {
              list(pmin=VariadicToBinary(pmin, pmin2),
                   pmax=VariadicToBinary(pmax, pmax2))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setMethod("as.character", "SimpleExpression", function(x) {
  x@expr
})


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Translation
###

setGeneric("translate", function(x, target, ...) standardGeneric("translate"))

setMethod("translate", c("language", "Expression"),
          function(x, target, ...) {
            as(eval(x, translationContext(target, x, ...)), class(target),
               strict=FALSE)
          })

setGeneric("translationContext",
           function(x, ...) standardGeneric("translationContext"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### General Solr translation
###

setMethod("forceBefore", c("SolrAggregateExpression", "SolrExpression"),
          function(x, y) TRUE)

setMethod("forceBefore", c("SolrSymbol", "SolrExpression"),
          function(x, y) TRUE)



### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "Expression", function(object) {
  cat(class(object), ": ", as.character(object), "\n", sep="")
})
