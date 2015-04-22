.test <- function() {
  BiocGenerics:::testPackage("rsolr")
}

uriPort <- function(x) {
  parseURI(x)$port
}

portIsOpen <- function(x) {
  con <- file()
  sink(con, type="message")
  on.exit({
    sink(NULL, type="message")
    close(con)
  })
  s <- suppressWarnings(make.socket(port = x, fail=FALSE))
  if (s$socket == 0L)
    FALSE
  else TRUE
}

getSolrHome <- function() {
  file.path(tempdir(), "solr")
}

populateSolrHome <- function(customSchema=NULL) {
  solr.home <- getSolrHome()
  file.copy(system.file("example-solr", "solr", package="rsolr"),
            dirname(solr.home), recursive=TRUE)
  if (!is.null(customSchema)) {
      sampleCorePath <- file.path(solr.home, "cores", "techproducts")
      corePath <- file.path(solr.home, "cores", name(customSchema))
      file.rename(sampleCorePath, corePath)
      saveXML(customSchema, file.path(corePath, "conf", "schema.xml"))
  }
  solr.home
}

getStartJar <- function() {
  system.file("example-solr", "start.jar", package="rsolr")
}

getLogConfFile <- function() {
    system.file("example-solr", "resources", "log4j.properties",
                package="rsolr")
}

buildCommandLine <- function() {
  paste("cd", dirname(getStartJar()), "; java",
        paste0("-Dsolr.solr.home=", getSolrHome()),
        paste0("-DSTOP.PORT=", 8079L),
        paste0("-DSTOP.KEY=", "rsolr"),
        paste0("-Dlog4j.configuration=file://", getLogConfFile()),
        "-jar", basename(getStartJar()))
}

setClassUnion("SolrSchemaORNULL", c("SolrSchema", "NULL"))

.ExampleSolr <-
  setRefClass("ExampleSolr",
              fields = list(
                uri = "character",
                customSchema = "SolrSchemaORNULL"
                ),
              methods = list(
                start = function() {
                  if (.self$isRunning()) {
                    warning("server already running (port is open)")
                    return()
                  }
                  solr.home <- populateSolrHome(.self$customSchema)
                  cmd <- buildCommandLine()
                  system(paste(cmd, "&"))
                  port <- uriPort(.self$uri)
                  while(!portIsOpen(port)) {
                    Sys.sleep(0.1)
                  }
                  Sys.sleep(1) # a bit more time to let it start
                  message("Solr started at: ", .self$uri)
                },
                kill = function() {
                  if (.self$isRunning()) {
                    unlink(getSolrHome(), recursive=TRUE)
                    system(paste(buildCommandLine(), "--stop"))
                  } else {
                    warning("Test solr not running")
                  }
                  port <- uriPort(uri)
                  while(portIsOpen(port)) {
                    Sys.sleep(0.1)
                  }
                  ##Sys.sleep(1) # wait a bit for it to stop
                },
                isRunning = function() {
                  portIsOpen(uriPort(uri))
                },
                finalize = function() {
                  if (.self$isRunning())
                    .self$kill()
                }))

TestSolr <- function(schema = NULL, start = TRUE)
{
  uri <- file.path("http://localhost:8983/solr",
                   if (is.null(schema)) "techproducts" else name(schema))
  solr <- .ExampleSolr$new(uri = uri, customSchema = schema)
  if (start) {
    solr$start()
  }
  solr
}

setMethod("show", "ExampleSolr", function(object) {
  cat("ExampleSolr instance\n")
  cat("uri: ", object$uri, "\n", sep="")
  if (!is.null(object$customSchema)) {
    cat("customSchema: ", name(object$customSchema), "\n", sep="")
  }
})
