checkResponseIdentical <- function(response, input) {
  checkIdentical(unmeta(response), input)
}

checkDFResponseEquals <- function(response, input, tolerance=1) {
  response <- unmeta(response)
  input <- as(input, "DocDataFrame")
  ids(input) <- NULL
  response <- response[colnames(response) %in% colnames(input)]
  checkEquals(response, input, tolerance=tolerance)
}

checkResponseEquals <- function(response, input, tolerance=1) {
  response[,"price_c"] <- NULL
  checkEquals(unmeta(response), input, tolerance=tolerance)
}

test_SolrFrame_accessors <- function() {
  solr <- rsolr:::TestSolr()
  s <- SolrFrame(solr$uri)

  checkEquals(SolrCore(solr$uri), core(s))
  checkIdentical(SolrQuery(), query(s))
  s[] <- NULL
  checkIdentical(nrow(s), 0L)
  checkIdentical(rownames(s), character())
  checkIdentical(colnames(s), fieldNames(core(s), includeStatic=TRUE))
  checkIdentical(ncol(s), length(colnames(s)))

  columnFields <- fields(schema(core(s)))[colnames(s)]
  storedColumns <- names(columnFields)[rsolr:::stored(columnFields)]

  doc <- data.frame(id="1112211111", name="my name!", stringsAsFactors=FALSE)
  s[doc$id,] <- doc
  dropped <- s[,,drop=TRUE]
  checkIdentical(names(dropped), storedColumns)
  checkTrue(is.integer(dropped$popularity))
  dropped[is.na(dropped)] <- NULL
  checkResponseIdentical(dropped, as.list(doc))
  
  checkIdentical(colnames(as.data.frame(s)), storedColumns)
  checkDFResponseEquals(as.data.frame(s), doc)
  checkTrue(is.integer(as.data.frame(s)$popularity))
  checkIdentical(nrow(s), 1L)
  checkIdentical(rownames(s), doc$id)
  
  docs <- list(
    list(id="2", price=2, inStock=TRUE, timestamp_dt=Sys.time()),
    list(id="3", price=3, inStock=FALSE, timestamp_dt=Sys.time()),
    list(id="4", price=4, timestamp_dt=Sys.time()),
    list(id="5", price=5, inStock=FALSE, timestamp_dt=Sys.time())
  )
  docs <- as(as(docs, "DocCollection"), "DocDataFrame")
  docs[,"timestamp_dt"] <- structure(docs[,"timestamp_dt"], tzone="UTC")
  docs[,"id"] <- as.character(docs[,"id"])
  ids(docs) <- docs[,"id"]
  s[,,insert=TRUE] <- docs
  
  allDocs <- docs
  allDocs$name <- NA
  allDocs <- allDocs[union(names(doc), names(allDocs))]
  allDocs <- rbind(allDocs,
                   cbind(as.data.frame(doc, row.names=doc$id),
                         price=NA, inStock=NA, timestamp_dt=NA))
  checkDFResponseEquals(as.data.frame(s), allDocs)

  checkDFResponseEquals(as.data.frame(s[as.character(2:4),]), docs[1:3,])
  checkIdentical(s[,"inStock"], c(docs[,"inStock"], NA))
  checkIdentical(s[rev(ids(docs)),"inStock"], rev(docs[,"inStock"]))

  s[["timestamp_dt"]] <- NULL
  checkIdentical(setdiff(fieldNames(s, onlyStored=TRUE), "price_c"),
                 setdiff(storedColumns, "timestamp_dt"))
  checkEquals(s[["timestamp_dt"]], as.POSIXct(rep(NA, 5)))
  s[["timestamp_dt"]] <- allDocs$timestamp_dt
  checkEquals(s[["timestamp_dt"]], allDocs$timestamp_dt, tolerance=1)
  s$price <- allDocs$price + 1L
  checkIdentical(s$price, allDocs$price + 1L)
  checkTrue(is(s["price"], "SolrFrame"))
  s[,"price"] <- allDocs$price
  checkIdentical(s[,"price"], allDocs$price)

  checkIdentical(colnames(as.data.frame(s[c("price", "i*")])),
                 c("price", "id", "includes", "inStock"))
  
  allDocs2 <- within(allDocs, {
    price <- price+1L
    inStock <- !inStock
  })
  s[c("price", "inStock")] <- allDocs2[c("price", "inStock")]
  rownames(allDocs2) <- NULL
  checkIdentical(as.data.frame(s[c("price", "inStock")]),
                 as(allDocs2[c("price", "inStock")], "DocDataFrame"))

  orig <- as.data.frame(s)
  s[] <- NULL
  checkIdentical(as.data.frame(s),
                 orig[NULL,intersect(colnames(orig), storedColumns)])

  s[] <- allDocs[1:2,]
  checkDFResponseEquals(as.data.frame(s), allDocs[1:2,])

  s[] <- allDocs
  stail <- tail(s, 2L)
  checkIdentical(nrow(stail), 2L)
  checkDFResponseEquals(as.data.frame(stail), tail(allDocs, 2L))

### TODO: check rename(), once fixed in Solr itself
}
