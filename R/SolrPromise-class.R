### =========================================================================
### SolrPromise objects
### -------------------------------------------------------------------------
###
### Reference to data stored in a Solr core (modified by some query)
###

setClass("SolrPromise",
         representation(expr="SolrExpression",
                        context="SolrFrameORNULL"),
         contains="SimplePromise")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

SolrPromise <- function(expr, context) {
    new("SolrPromise", expr=expr, context=context)
}

setMethod("Promise", c("SolrExpression", "RSolrContext"),
          function(expr, context) {
              SolrPromise(expr, solr(context))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Fulfillment
###
