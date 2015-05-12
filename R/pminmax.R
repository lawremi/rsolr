### =========================================================================
### Override of pmin/max to support binary dispatch
### -------------------------------------------------------------------------
###
### The dispatch will degrade the performance of the classic
### do.call(pmin, as.data.frame(m)) trick, but maybe we should get
### Biobase::rowQ into base R?
###

setGeneric("pmax2", function(x, y, na.rm=FALSE) standardGeneric("pmax2"),
           signature=c("x", "y"))

setMethod("pmax2", "ANY", function(x, y, na.rm=FALSE) {
              BiocGenerics::pmax(x, y, na.rm=na.rm)
          })

setGeneric("pmin2", function(x, y, na.rm=FALSE) standardGeneric("pmin2"),
           signature=c("x", "y"))

setMethod("pmin2", "ANY", function(x, y, na.rm=FALSE) {
              BiocGenerics::pmin(x, y, na.rm=na.rm)
          })

pmin <- VariadicToBinary(pmin, pmin2)
pmax <- VariadicToBinary(pmax, pmax2)
