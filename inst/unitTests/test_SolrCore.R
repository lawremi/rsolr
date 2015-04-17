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
  checkIdentical(name(sc), "example")
  checkIdentical(ndoc(sc), 0L)
  checkIdentical(uniqueKey(schema(sc)), "id")
  
  doc <- list(id="1112211111", name="my name!")
  dc <- as(list(doc), "DocCollection")
  update(sc, dc, commit=FALSE)
  q <- SolrQuery(id == .(doc$id))
  checkIdentical(read(sc, q), dc[NULL])

  checkIdentical(commit(sc), 0L)
  checkIdentical(read(sc, q), dc[NULL]) # still NULL for 30s because of cache
  purgeCache(sc)
  ids(dc) <- doc$id
  checkResponseIdentical(read(sc, q), dc)
  checkIdentical(ndoc(sc), length(dc))
  
  docs <- list(
    list(id="2", inStock=TRUE, price=2, timestamp_dt=Sys.time()),
    list(id="3", inStock=FALSE, price=3, timestamp_dt=Sys.time()),
    list(id="4", price=4, timestamp_dt=Sys.time()),
    list(id="5", inStock=FALSE, price=5, timestamp_dt=Sys.time())
    )

  update(sc, docs)

  docs <- as(docs, "DocCollection")
  docs[,"timestamp_dt"] <- structure(docs[,"timestamp_dt"], tzone="UTC")
  ids(docs) <- docs[,"id"]

  q <- SolrQuery(id %in% as.character(2:4))
  checkResponseEquals(read(sc, q), docs[1:3])

  q <- SolrQuery(id == .(doc$id))
  delete(sc, q)
  checkResponseEquals(read(sc), docs)
  del <- list()
  del[as.character(2:4)] <- list(NULL)
  update(sc, del)
  purgeCache(sc)
  checkResponseEquals(read(sc), docs[4])
  delete(sc)
  purgeCache(sc)
  checkResponseEquals(read(sc), new("DocList"))

  docs[,"id"] <- NULL
  update(sc, docs)
  docs[,"id"] <- ids(docs)
  purgeCache(sc)
  checkResponseEquals(read(sc), docs)

  del <- c(del, list(doc))
  update(sc, del)
  purgeCache(sc)
  checkResponseEquals(read(sc), c(dc, docs["5"]))

  solr$kill()
}
