### =========================================================================
### Context objects for Solr
### -------------------------------------------------------------------------
###

setClass("RSolrContext",
         representation(solr="SolrFrame"),
         contains="RContext")

RSolrContext <- function(env, solr) {
    new("RSolrContext", env, solr=solr)
}
