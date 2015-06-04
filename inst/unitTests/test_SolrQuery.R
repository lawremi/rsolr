## Some inspiration for these tests:
## https://svn.apache.org/repos/asf/lucene/dev/trunk/solr/solrj/src/test/org/apache/solr/client/solrj/SolrExampleTests.java

checkResponseIdentical <- function(response, input) {
  checkIdentical(unmeta(response), input)
}

checkResponseEquals <- function(response, input, tolerance=1) {
  response[,"price_c"] <- NULL
  checkEquals(unmeta(response), input, tolerance=tolerance)
}

library(rsolr)
library(RUnit)
options(verbose=TRUE)

test_SolrQuery <- function() {
  solr <- rsolr:::TestSolr()
  s <- SolrList(solr$uri)
  sc <- core(s)

  docs <- list(
    list(id="2", inStock=TRUE, price=2, weight=1, timestamp_dt=Sys.time()),
    list(id="3", inStock=TRUE, price=4, weight=5, timestamp_dt=Sys.time()),
    list(id="4", price=3, weight=3, timestamp_dt=Sys.time()),
    list(id="5", inStock=FALSE, price=5, timestamp_dt=Sys.time())
    )
  s[] <- docs

  docs <- as(docs, "DocCollection")
  docs[,"timestamp_dt"] <- structure(docs[,"timestamp_dt"], tzone="UTC")
  ids(docs) <- as.character(rsolr:::pluck(docs, "id"))

  testSubset(s, docs)
  testBoundsRestriction(sc, docs)
  testSort(sc, docs)
  testTransform(sc, docs)
  testFacets(sc, docs)
  
  solr$kill()
}

testSubset <- function(s, docs) {
    query <- SolrQuery()
    sc <- core(s)
    subset.query <- subset(query, id %in% 3:5)
    checkResponseEquals(read(sc, subset.query), docs[2:4])
    subset.query <- subset(subset.query, fields=c("id", "inStock"))
    checkResponseEquals(read(sc, subset.query),
                        docs[2:4, c("id", "inStock")])
    subset.query <- subset(subset.query, fields="*")
    checkResponseEquals(read(sc, subset.query), docs[2:4])
    subset.query <- subset(subset.query, inStock)
    checkResponseEquals(read(sc, subset.query), docs[2])

    query <- SolrQuery()
    subset.query <- subset(query, price > 2)
    checkResponseEquals(read(sc, subset.query), docs[2:4])
    subset.query <- subset(query, price < 5)
    checkResponseEquals(read(sc, subset.query), docs[1:3])
    subset.query <- subset(query, price <= 4)
    checkResponseEquals(read(sc, subset.query), docs[1:3])
    subset.query <- subset(query, price >= 5)
    checkResponseEquals(read(sc, subset.query), docs[4])
    subset.query <- subset(query, price > 2 & price < 5)
    checkResponseEquals(read(sc, subset.query), docs[2:3])
    subset.query <- subset(query, price <= 2 | price >= 5)
    checkResponseEquals(read(sc, subset.query), docs[c(1,4)])
    subset.query <- subset(query, !(price <= 2 | price >= 5))
    checkResponseEquals(read(sc, subset.query), docs[2:3])
    subset.query <- subset(query, (price + 1) > 3)
    checkResponseEquals(read(sc, subset.query), docs[2:4])
    subset.query <- subset(query, is.na(inStock))
    checkResponseEquals(read(sc, subset.query), docs[3])
    subset.query <- subset(query, grepl("[0-4]", id))
    checkResponseEquals(read(sc, subset.query), docs[1:3])
    subset.query <- subset(query, grepl("[0-4]", id, fixed=TRUE))
    checkResponseEquals(read(sc, subset.query), unname(docs[NULL]))
    subset.query <- subset(query, grepl("2", id, fixed=TRUE))
    checkResponseEquals(read(sc, subset.query), docs[1])
    subset.query <- subset(query, (price + 1) > 3 & (price - 1) < 4)
    checkResponseEquals(read(sc, subset.query), docs[2:3])
    subset.query <- subset(query, price > 3 & FALSE)
    checkResponseEquals(read(sc, subset.query), unname(docs[NULL]))
    subset.query <- subset(query, TRUE & price > 3)
    checkResponseEquals(read(sc, subset.query), docs[c(2,4)])
    subset.query <- subset(query, TRUE | price > 3)
    checkResponseEquals(read(sc, subset.query), docs)
    subset.query <- subset(query, price > weight)
    checkResponseEquals(read(sc, subset.query), docs[1])
    subset.query <- subset(query, !(price > weight))
    checkResponseEquals(read(sc, subset.query), docs[2:3])
    subset.query <- subset(query, price <= weight)
    checkResponseEquals(read(sc, subset.query), docs[2:3])
    subset.query <- subset(query, price < weight)
    checkResponseEquals(read(sc, subset.query), docs[2])
    subset.query <- subset(query, price >= weight)
    checkResponseEquals(read(sc, subset.query), docs[c(1,3)])
    subset.query <- subset(query, price %/% weight == 0)
    checkResponseEquals(read(sc, subset.query), docs[2])
    subset.query <- subset(query, trunc(-(weight/price)) == -1)
    checkResponseEquals(read(sc, subset.query), docs[2:3])

    joindocs <- as(list(
        list(id="6", links="4"),
        list(id="7", links="2")
        ), "DocList")
    names(joindocs) <- joindocs[,"id"]
    s[ids(joindocs)] <- joindocs
    subset.query <- subset(query, links %in% id[price > 2])
    checkResponseEquals(read(sc, subset.query), joindocs[1])
    subset.query <- subset(query, links %in% id)
    checkResponseEquals(read(sc, subset.query), joindocs)

    s[] <- docs
}

testBoundsRestriction <- function(sc, docs) {
    query <- SolrQuery()
    head.query <- head(query, 2L)
    checkResponseEquals(read(sc, head.query), head(docs, 2L))
    tail.query <- tail(query, 2L)
    checkResponseEquals(read(sc, tail.query), tail(docs, 2L))
    tail.query <- tail(query, -3L)
    checkResponseEquals(read(sc, tail.query), tail(docs, -3L))
    head.query <- head(query, -3L)
    checkResponseEquals(read(sc, head.query), head(docs, -3L))
    restricted.query <- tail(head(query, -2L), 1L)
    checkResponseEquals(read(sc, restricted.query), tail(head(docs, -2L), 1L))
    restricted.query <- head(tail(query, -2L), 1L)
    checkResponseEquals(read(sc, restricted.query), head(tail(docs, -2L), 1L))
    restricted.query <- head(tail(query, 2L), -1L)
    checkResponseEquals(read(sc, restricted.query), head(tail(docs, 2L), -1L))
    restricted.query <- tail(head(query, 2L), -1L)
    checkResponseEquals(read(sc, restricted.query), tail(head(docs, 2L), -1L))
    restricted.query <- head(tail(query, -1L), 2L)
    checkResponseEquals(read(sc, restricted.query), head(tail(docs, -1L), 2L))
    restricted.query <- tail(head(query, -2L), -1L)
    checkResponseEquals(read(sc, restricted.query), tail(head(docs, -2L), -1L))
    restricted.query <- tail(head(query, -1L), -2L)
    checkResponseEquals(read(sc, restricted.query), tail(head(docs, -1L), -2L))
    restricted.query <- head(tail(query, -2L), -1L)
    checkResponseEquals(read(sc, restricted.query), head(tail(docs, -2L), -1L))
    restricted.query <- head(tail(query, 2L), 1L)
    checkResponseEquals(read(sc, restricted.query), head(tail(docs, 2L), 1L))
    restricted.query <- tail(head(query, 2L), 1L)
    checkResponseEquals(read(sc, restricted.query), tail(head(docs, 2L), 1L))
}

testSort <- function(s, docs) {
    query <- SolrQuery()
    sc <- core(s)
    sorted.query <- sort(query, by = ~ price)
    checkResponseEquals(read(sc, sorted.query), docs[c(1, 3, 2, 4)])
    sorted.query <- sort(query, by = ~ price, decreasing=TRUE)
    checkResponseEquals(read(sc, sorted.query), docs[c(4, 2, 3, 1)])
    reverse.query <- rev(sorted.query)
    checkResponseEquals(read(sc, reverse.query), rev(docs[c(4, 2, 3, 1)]))
    sorted.query <- sort(query, by = ~ I(weight * -1))
    checkResponseEquals(read(sc, sorted.query), docs[c(2, 3, 1, 4)])
    sorted.query <- sort(query, by = ~ I(weight * -1), decreasing=TRUE)
    checkResponseEquals(read(sc, sorted.query), docs[c(1, 3, 2, 4)])
    sorted.query <- sort(query, by = ~ id, decreasing=TRUE)
    checkResponseEquals(read(sc, sorted.query), rev(docs))
}

testTransform <- function(sc, docs) {
    query <- SolrQuery()

    tform.query <- transform(query, stocked=inStock)
    tform.docs <- docs
    tform.docs[,"stocked"] <- tform.docs[,"inStock"]
    tform.docs["4","stocked"] <- NULL
    checkResponseEquals(read(sc, tform.query), tform.docs)

    tform.query <- transform(query, membership = id %in% 3:5)
    tform.docs <- docs
    tform.docs[,"membership"] <- tform.docs[,"id"] %in% 3:5
    checkResponseEquals(read(sc, tform.query), tform.docs)
    
    tform.query <- transform(query, overpriced=price > weight)
    tform.docs <- docs
    tform.docs[,"overpriced"] <- tform.docs[,"price"] > tform.docs[,"weight"]
    tform.docs["5","overpriced"] <- NULL
    checkResponseEquals(read(sc, tform.query), tform.docs)
    
    tform.query <- transform(query, negPrice = price * -1)
    tform.docs <- docs
    tform.docs[,"negPrice"] <- tform.docs[,"price"] * -1
    checkResponseEquals(read(sc, tform.query), tform.docs)

    tform.query <- transform(query, price = price * -1)
    tform.docs <- docs
    tform.docs[,"price"] <- tform.docs[,"price"] * -1
    checkResponseEquals(read(sc, tform.query), tform.docs)
    
    tform.query <- transform(query, price = round(price/3))
    tform.docs <- docs
    tform.docs[,"price"] <- round(tform.docs[,"price"]/3)
    checkResponseEquals(read(sc, tform.query), tform.docs)

    tform.query <- transform(query, pmin = pmin(price, weight),
                             pmax = pmax(price, weight))
    tform.docs <- docs
    tform.docs[,"pmin"] <- pmin(tform.docs[,"price"], tform.docs[,"weight"])
    tform.docs[,"pmax"] <- pmin(tform.docs[,"price"], tform.docs[,"weight"])
    tform.docs["5",c("pmax", "pmin")] <- NULL
    checkResponseEquals(read(sc, tform.query), tform.docs)

    tform.query <- transform(query, pmin = pmin(price, weight, na.rm=TRUE),
                             pmax = pmax(price, weight, na.rm=TRUE))
    tform.docs <- docs
    tform.docs[,"pmin"] <- pmin(tform.docs[,"price"], tform.docs[,"weight"],
                                na.rm=TRUE)
    tform.docs[,"pmax"] <- pmin(tform.docs[,"price"], tform.docs[,"weight"],
                                na.rm=TRUE)
    checkResponseEquals(read(sc, tform.query), tform.docs)
}

testFacets <- function(sc, docs) {
    checkTable <- function(formula, counts, query = SolrQuery())
        {
            expr.lang <- attr(terms(formula), "variables")[[2]]
            expr.name <- deparse(eval(call("bquote", expr.lang)))
            facet.query <- xtabs(formula, query)
            fct <- as.table(facets(sc, facet.query)[[expr.name]])
            correct.fct <- as.table(counts)
            dimnames(correct.fct) <- setNames(list(names(counts)), expr.name)
            checkIdentical(fct, correct.fct)
        }

    checkTable(~ inStock, c("FALSE"=1L, "TRUE"=2L))
    checkTable(~ !inStock, c("FALSE"=2L, "TRUE"=1L))
### SOLRBUG: numeric faceting returns 0.0:0 buckets using our params
### https://issues.apache.org/jira/browse/SOLR-7496
###    checkTable(~ price, c("2.0"=1L, "3.0"=1L, "4.0"=1L, "5.0"=1L))
    checkTable(~ price > 3, c("FALSE"=2L, "TRUE"=2L))
    checkTable(~ price > 3 & inStock, c("FALSE"=3L, "TRUE"=1L))
    checkTable(~ price > log2(8), c("FALSE"=2L, "TRUE"=2L))
    checkTable(~ cut(price, seq(1, 5, 2)), c("(1,3]"=2L, "(3,5]"=2L))
### FIXME: waiting on interval facets in the JSON API
    ## checkTable(~ cut(price, c(-Inf, 2, 4, 5, Inf)),
    ##            c("(-Inf,2]"=1L, "(2,4]"=2L, "(4,5]"=1L, "(5, Inf]"=0L))
    query <- SolrQuery()
    tform.query <- transform(query, bins = cut(price, seq(1, 5, 2)))
    checkTable(~ bins, c("(1,3]"=2L, "(3,5]"=2L), query=tform.query)
### FIXME: SOLR bug 7496
### facet.query <- xtabs(~ price, query)
    facet.query <- query
    facet.query <- xtabs(~ inStock, facet.query)
    facet.query <- xtabs(~ price > 2 + 1, facet.query)
    facet.query <- xtabs(~ price - 2 > 1, facet.query)
    fct <- facets(sc, facet.query)
    ## tab <- as.table(c("2.0"=1L, "3.0"=1L, "4.0"=1L, "5.0"=1L))
    ## names(dimnames(tab)) <- "price"
    ## checkIdentical(fct$price,  tab)
    tab <- as.table(c("FALSE"=1L, "TRUE"=2L))
    names(dimnames(tab)) <- "inStock"
    checkIdentical(as.table(fct$inStock),  tab)
    tab <- as.table(c("FALSE"=2L, "TRUE"=2L))
    names(dimnames(tab)) <- "price > 2 + 1"
    checkIdentical(fct$"price > 2 + 1",  tab)
    names(dimnames(tab)) <- "price - 2 > 1"
    checkIdentical(fct$"price - 2 > 1", tab)
    checkIdentical(length(facet(sc, query)), 0L)
    facet.query <- sort(xtabs(~ inStock, query), decreasing=TRUE)
    fct <- facet(sc, facet.query)
    tab <- as.table(c("TRUE"=3L, "FALSE"=1L))
    names(dimnames(tab)) <- "inStock"
    checkIdentical(fct$inStock,  tab)
    
    ## CHECK: xtabs() pivoting
    xtabs.query <- xtabs(~ price + inStock, query)
    fct <- facet(sc, xtabs.query)$"price,inStock"
    correct.fct <- as.table(matrix(as.integer(c(0,0,0,1,1,1,1,0)), 4, 2))
    dimnames(correct.fct) <- list(price=as.character(2:5),
                                  inStock=c("FALSE", "TRUE"))
    checkIdentical(fct, correct.fct)

    ## CHECK: facet on restricted query
    sub.query <- subset(query, inStock)
    checkTable(~ inStock, c("FALSE"=0L, "TRUE"=3L), sub.query)

    ## CHECK: df upload and statistics
    val_pi <- c(23, 26, 38, 46, 55, 63, 77, 84, 92, 94)
    price <- val_pi^2
    df <- data.frame(id=seq_along(val_pi), name=paste0("doc:", val_pi), val_pi,
                     price, inStock=rep(c(TRUE, FALSE), length=length(val_pi)))
    s[] <- df

    stats.query <- stats(query, "val_pi")
    stats.df <- as.data.frame(stats(sc, stats.query))
    statsDf <- function(x, field=deparse(substitute(x))) {
        data.frame(field, min=min(x), max=max(x),
                   sum=sum(x), count=as.numeric(length(x)),
                   missing=as.numeric(sum(is.na(x))),
                   sumOfSquares=sum(x^2), mean=mean(x),
                   stddev=sd(x), row.names=field, stringsAsFactors=FALSE)
    }
    correct.stats.df <- statsDf(val_pi)
    checkIdentical(stats.df, correct.stats.df)

    stats.query <- stats(query, val_pi ~ .)
    stats.df <- as.data.frame(stats(sc, stats.query))
    checkIdentical(stats.df, correct.stats.df)

    stats.query <- stats(query, c(val_pi, price) ~ .)
    stats.df <- as.data.frame(stats(sc, stats.query))
    correct.stats.df2 <- rbind(correct.stats.df, statsDf(price))
    checkIdentical(stats.df, correct.stats.df2 )
    
    stats.query <- stats(query, val_pi ~ inStock)
    stats.df <- as.data.frame(stats(sc, stats.query)[val_pi ~ inStock])
    correct.facet.df <- do.call(rbind, tapply(val_pi, df$inStock, statsDf,
                                              field="val_pi"))
    correct.facet.df$field <- NULL
    correct.facet.df <- cbind(inStock=rownames(correct.facet.df),
                              correct.facet.df)
    checkIdentical(stats.df, correct.facet.df)
}
