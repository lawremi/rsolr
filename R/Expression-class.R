### =========================================================================
### Expression objects
### -------------------------------------------------------------------------
###
### Represents a target of the translate() generic.
###

setClassUnion("Expression", "language")

setClass("SimpleExpression",
         representation(expr="character"),
         prototype(expr=""),
         contains="Expression",
         validity=function(object) {
           if (!isSingleString(object@expr)) {
             "'expr' must be a single, non-NA string"
           }
         })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Symbols
###
### We define a Symbol as an atom in a language that refers to a field
### or variable. A specific type of Symbol is mapped to a specific
### type of Promise, which in turn generates a specific type of
### Expression.
###

setClassUnion("Symbol", "name")
setIs("Symbol", "Expression")

setClass("SimpleSymbol",
         representation(name="character"),
         validity=function(object) {
             if (!isSingleString(object@name))
                 "'name' must be a single, non-NA string"
         })
setIs("SimpleSymbol", "Symbol")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Translation
###

setGeneric("translate", function(x, target, ...) standardGeneric("translate"))

setMethod("translate", c("ANY", "Expression"),
          function(x, target, context, ...) {
              if (is(x, class(target))) {
                  return(x)
              }
              symbolFactory(context) <- SymbolFactory(target, ...)
              translation <- eval(x, context)
              if (is(translation, "Promise")) {
                  if (!compatible(context(translation), frame(context))) {
                      stop("target context incompatible with source context")
                  }
                  translation <- expr(translation)
              }
              as(translation, class(target), strict=FALSE)
          })

setClass("SymbolFactory", contains = "function")

setClass("TranslationRequest",
         representation(src="Expression",
                        target="Expression"))

setMethod("as.character", "TranslationRequest", function(x) as.character(x@src))

setMethod("show", "TranslationRequest", function(object) {
              cat("'", as.character(object@src), "' => '", class(object@target),
                  "'\n", sep="")
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities
###

preprocessExpression <- function(expr, env) {
    while(!identical(expr, expr <- bquote2(expr, env))) { }
    callsToNames(expr, quote(.field), env)
}

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
