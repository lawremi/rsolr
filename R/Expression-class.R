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

setClass("Symbol", contains="Expression")

setClass("SimpleSymbol",
         representation(name="character"),
         contains="Symbol")

setGeneric("Symbol", function(name, target) standardGeneric("Symbol"))

setMethod("Symbol", c("character", "ANY"), function(name, target) {
              Symbol(as.name(name), target)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Translation
###

setGeneric("translate", function(x, target, ...) standardGeneric("translate"))

setMethod("translate", c("language", "Expression"),
          function(x, target, ...) {
              as(eval(x, translationContext(x, target, ...)), class(target),
                 strict=FALSE)
          })

setGeneric("translationContext",
           function(x, target, ...) standardGeneric("translationContext"))

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
### environment to handle special cases. Obviously, if the objects are
### used in a different scope, things will break, so we have to keep
### the function overrides to a minimum.
###

PromiseEnv <- function(x, target, context, parent) {
    vars <- all.vars(x)
    syms <- lapply(vars, Symbol, target)
    syms <- setNames(lapply(vars, Promise, context), vars)
    list2env(syms, parent=parent)
}

FunsEnv <- function(target, parent) {
    list2env(overrides(target), parent=parent)
}

setMethod("translationContext", c("language", "Expression"),
          function(x, target, parent = emptyenv()) {
              PromiseEnv(x, target, parent, FunsEnv(target, parent))
          })

setGeneric("overrides", function(x) standardGeneric("overrides"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setMethod("as.character", "SimpleExpression", function(x) {
  x@expr
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "Expression", function(object) {
  cat(class(object), ": ", as.character(object), "\n", sep="")
})
