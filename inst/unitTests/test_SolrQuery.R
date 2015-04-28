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

test_SolrQuery <- function() {
  solr <- rsolr:::TestSolr()
  s <- SolrList(solr$uri)
  sc <- core(s)

  docs <- list(
    list(id="2", inStock=TRUE, price=2, timestamp_dt=Sys.time()),
    list(id="3", inStock=TRUE, price=4, timestamp_dt=Sys.time()),
    list(id="4", inStock=TRUE, price=3, timestamp_dt=Sys.time()),
    list(id="5", inStock=FALSE, price=5, timestamp_dt=Sys.time())
    )
  s[] <- docs

  docs <- as(docs, "DocCollection")
  docs[,"timestamp_dt"] <- structure(docs[,"timestamp_dt"], tzone="UTC")
  ids(docs) <- as.character(rsolr:::pluck(docs, "id"))

  ## CHECK: subset() queries
  query <- SolrQuery()
  subset.query <- subset(query, id %in% 2:4)
  checkResponseEquals(read(sc, subset.query), docs[1:3])
  subset.query <- subset(subset.query, fields=c("id", "inStock"))
  checkResponseEquals(read(sc, subset.query), docs[1:3, c("id", "inStock")])
  ## TODO: relational, arithmetic, compound logical, vs. summaries
  
  ## CHECK: bounds restriction
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
  
  ## CHECK: sort
  query <- SolrQuery()
  sorted.query <- sort(query, by = ~ price)
  checkResponseEquals(read(sc, sorted.query), docs[c(1, 3, 2, 4)])

  sorted.query <- sort(query, by = ~ price, decreasing=TRUE)
  checkResponseEquals(read(sc, sorted.query), docs[c(4, 2, 3, 1)])
  reverse.query <- rev(sorted.query)
  checkResponseEquals(read(sc, reverse.query), rev(docs[c(4, 2, 3, 1)]))  
  sorted.query <- sort(query, by = ~ I(price * -1))
  checkResponseEquals(read(sc, sorted.query), docs[c(4, 2, 3, 1)])
  
  ## CHECK: transform
  tform.query <- transform(query, negPrice = price * -1)
  tform.docs <- docs
  tform.docs[,"negPrice"] <- tform.docs[,"price"] * -1
  checkResponseEquals(read(sc, tform.query), tform.docs)
  tform.query <- transform(query, price = price * -1)
  tform.docs <- docs
  tform.docs[,"price"] <- tform.docs[,"price"] * -1
  checkResponseEquals(read(sc, tform.query), tform.docs)
  tform.query <- transform(query, inStock2=inStock, stocked=inStock)
  tform.query <- subset(tform.query, fields="i*")
  tform.docs <- docs
  tform.docs[,"inStock2"] <- tform.docs[,"inStock"]
  checkResponseEquals(read(sc, tform.query),
                      tform.docs[,c("id", "inStock", "inStock2")])

  testFacets(sc)
  testFacets(sc, "5.1")
  
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

  solr$kill()
}

testFacets <- function(sc, version="") {
    ## CHECK: facet
    checkFacet <- function(formula, counts, query = SolrQuery(version=version))
    {
        expr.lang <- attr(terms(formula), "variables")[[2]]
        if (is.call(expr.lang) && expr.lang[[1]] == quote(cut)) {
            expr.name <- as.character(expr.lang[[2]])
        } else {
            expr.name <- deparse(eval(call("bquote", expr.lang)))
        }
        facet.query <- xtabs(formula, query)
        fct <- facet(sc, facet.query)[[expr.name]]
        correct.fct <- as.table(counts)
        dimnames(correct.fct) <- setNames(list(names(counts)), expr.name)
        checkIdentical(fct, correct.fct)
    }

    checkFacet(~ inStock, c("FALSE"=1L, "TRUE"=3L))
    checkFacet(~ !inStock, c("FALSE"=3L, "TRUE"=1L))
    checkFacet(~ price, c("2.0"=1L, "3.0"=1L, "4.0"=1L, "5.0"=1L))
    checkFacet(~ price > 3, c("FALSE"=2L, "TRUE"=2L))
    checkFacet(~ price > 3 & inStock, c("FALSE"=3L, "TRUE"=1L))
    checkFacet(~ price > log2(8), c("FALSE"=2L, "TRUE"=2L))
    three <- 3
    checkFacet(~ price > .(three), c("FALSE"=2L, "TRUE"=2L))
    checkFacet(~ cut(price, seq(1, 5, 2)), c("(1,3]"=2L, "(3,5]"=2L))
    checkFacet(~ cut(price, c(-Inf, 2, 4, 5, Inf)),
               c("(-Inf,2]"=1L, "(2,4]"=2L, "(4,5]"=1L, "(5, Inf]"=0L))
    query <- SolrQuery(version=version)
    facet.query <- xtabs(~ price, query)
    facet.query <- xtabs(~ inStock, facet.query)
    facet.query <- xtabs(~ price > 2 + 1, facet.query)
    facet.query <- xtabs(~ price - 2 > 1, facet.query)
    fct <- facet(sc, facet.query)
    tab <- as.table(c("2.0"=1L, "3.0"=1L, "4.0"=1L, "5.0"=1L))
    names(dimnames(tab)) <- "price"
    checkIdentical(fct$price,  tab)
    tab <- as.table(c("FALSE"=1L, "TRUE"=3L))
    names(dimnames(tab)) <- "inStock"
    checkIdentical(fct$inStock,  tab)
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
    checkFacet(~ inStock, c("FALSE"=0L, "TRUE"=3L), sub.query)

    if (as.package_version(version) >= "5.1") {
        testAdvancedFacets()
    }
}

testAdvancedFacets <- function() {
}
