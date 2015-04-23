### =========================================================================
### Context objects for Solr
### -------------------------------------------------------------------------
###

setClassUnion("SolrFrameORNULL", c("SolrFrame", "NULL"))

setClass("RSolrContext",
         representation(solr="SolrFrameORNULL"),
         contains="RContext")

RSolrContext <- function(env, solr) {
    if (!fulfillable(solr)) {
        solr <- NULL
    } else {
        solr <- as(solr, "SolrFrame")
    }
    new("RSolrContext", as(env, "RContext"), solr=solr)
}
