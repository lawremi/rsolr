library(rsolr)
library(RUnit)

checkResponseIdentical <- function(response, input) {
  checkIdentical(unmeta(response), input)
}

checkResponseEquals <- function(response, input, tolerance=1) {
  response[,"price_c"] <- NULL
  checkEquals(unmeta(response), input, tolerance=tolerance)
}

test_SolrCore_accessors <- function() {
  solr <- rsolr:::TestSolr()
  sc <- SolrCore(solr$uri)

  checkIdentical(uniqueKey(schema(sc)), "id")
  
  doc <- list(id="1112211111", name="my name!")
  sc[[doc$id,commit=FALSE]] <- doc
  checkIdentical(sc[[doc$id]], NULL)
  checkIdentical(commit(sc), 0L)
  checkIdentical(sc[[doc$id]], NULL) # still NULL for 30s because of cache
  purgeCache(sc)
  checkResponseIdentical(sc[[doc$id]], doc)

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
  
  checkResponseEquals(sc[as.character(2:4)], docs[1:3])

  sc[[doc$id]] <- NULL
  checkResponseEquals(sc[], docs)
  sc[as.character(2:4)] <- NULL
  purgeCache(sc)
  checkResponseEquals(sc[], docs[4])
  delete(sc)
  purgeCache(sc)
  checkResponseEquals(sc[], new("DocList"))

  docs[,"id"] <- NULL
  sc[] <- docs
  docs[,"id"] <- ids(docs)
  purgeCache(sc)
  checkResponseEquals(sc[], docs)
  delete(sc)

  ids <- ids(docs)
  ids(docs) <- NULL
  docs[,"id"] <- NULL
  sc[ids,] <- docs
  docs[,"id"] <- ids
  purgeCache(sc)
  checkResponseEquals(sc[], docs)
  
  checkIdentical(sc["foo"], new("DocList"))
  checkIdentical(sc[["foo"]], NULL)
  
  solr$kill()
}
