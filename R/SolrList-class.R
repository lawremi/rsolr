### =========================================================================
### SolrList objects
### -------------------------------------------------------------------------
###
### High-level object that represents a Solr core as a list, with benefits
###

setClass("SolrList",
         representation(core="SolrCore",
                        query="SolrQuery"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

.SolrList <- function(core, query=SolrQuery()) {
  new("Solr", core=core, query=query)
}

SolrList <- function(uri, ...) {
  .SolrList(SolrCore(uri, ...))
}


