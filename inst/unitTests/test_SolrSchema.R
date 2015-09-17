test_SolrSchema_creation <- function() {
  data(Cars93, package="MASS")
  Cars93$Man.trans.avail <- Cars93$Man.trans.avail == "Yes"
  Cars93$Model <- as.character(Cars93$Model)

  schema <- deriveSolrSchema(Cars93)
  checkIdentical(name(schema), "Cars93")
  doc <- as(schema, "XMLDocument")
  checkIdentical(XML::saveXML(doc), XML::saveXML(schema))

  solr <- TestSolr(schema, restart=TRUE)
  sr <- SolrFrame(solr$uri)
  sr[] <- Cars93
  ## Differences: factors lost
  checkDfIdentical <- function(df, truth) {
    factorCols <- vapply(truth, is.factor, logical(1L))
    df[factorCols] <- mapply(factor, df[factorCols],
                             lapply(truth[factorCols], levels), SIMPLIFY=FALSE)
    checkIdentical(truth, df)
  }
  Cars93_ddf <- as(Cars93, "DocDataFrame")
  checkDfIdentical(as.data.frame(sr), Cars93_ddf)
### TODO: check single column extraction
  
  schema <- deriveSolrSchema(Cars93, uniqueKey="Model")
  solr <- TestSolr(schema, restart=TRUE)
  sr <- SolrFrame(solr$uri)
  sr[] <- Cars93
  df <- as.data.frame(sr["Integra",])
  df$Model <- as.character(df$Model)
  rownames(df) <- NULL
  checkDfIdentical(df, droplevels(subset(Cars93, Model=="Integra")))
}
