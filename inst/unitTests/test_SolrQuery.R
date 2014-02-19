## Inspiration for these tests:
## https://svn.apache.org/repos/asf/lucene/dev/trunk/solr/solrj/src/test/org/apache/solr/client/solrj/SolrExampleTests.java

library(rsolr)
library(RUnit)

checkResponseIdentical <- function(response, input) {
  checkIdentical(unmeta(response), input)
}

checkResponseEquals <- function(response, input, tolerance=1) {
  response[,"price_c"] <- NULL
  checkEquals(unmeta(response), input, tolerance=tolerance)
}

test_SolrQuery <- function() {
  solr <- rsolr:::TestSolr()
  sc <- SolrCore(solr$uri)

  docs <- list(
    list(id="2", inStock=TRUE, price=2, timestamp_dt=Sys.time()),
    list(id="3", inStock=FALSE, price=3, timestamp_dt=Sys.time()),
    list(id="4", inStock=TRUE, price=4, timestamp_dt=Sys.time()),
    list(id="5", inStock=FALSE, price=5, timestamp_dt=Sys.time())
    )
  sc[] <- docs

  docs <- as(docs, "DocCollection")
  docs[,"timestamp_dt"] <- structure(docs[,"timestamp_dt"], tzone="UTC")
  ids(docs) <- as.character(rsolr:::pluck(docs, "id"))
  
  query <- SolrQuery()
  query <- subset(query, id %in% 2:4)
  checkResponseEquals(sc[query], docs[1:3])

  ## CHECK: all these facet parameters
  
  query <- SolrQuery()
  facet.query <- facet(~ inStock, query)
  params <- query@params
  params$rows <- 0L
  params <- c(params, facet.field="inStock", facet.limit=-1L, facet.sort="index")
  checkIdentical(facet.query@params, params)
  facets(sc, query)$inStock
  
  facet.query <- facet(~ !inStock, query)
  names(params)[names(params) == "facet.field"] <- "facet.query"
  params$facet.query <- "-inStock:true"
  facet.query <- facet(~ price, query)
  facet.query <- facet(~ price > 3, query)
  facet.query <- facet(~ price > 3 & inStock, query)
  facet.query <- facet(~ price > log2(8), query)
  three <- 3
  facet.query <- facet(~ price > .(three), query)
  facet.query <- facet(~ cut(price, seq(1, 5, 2), query))
  facet.query <- facet(~ cut(price, c(-Inf, 2, 4, 5, Inf)), query)
  facet.query <- facet(~ price + inStock, query)
  facet.query <- facet(~ price > I(2+1) + inStock, query)

  
  ## CHECK: correct query result
  facet.ans <- sc[facet(~ inStock, query)]
  facet.ans <- sc[facet(~ price > 3, query)]
  facet.ans <- sc[facet(~ price + inStock, query)]

  ## CHECK: xtabs() yields same query for simple case
  xtabs.query <- xtabs(~ inStock, query)
  
  ## CHECK: xtabs() pivoting
  xtabs.query <- xtabs(~ price + inStock, query)
  
}
