.test <- function() {
  BiocGenerics:::testPackage("rsolr")
}

uriPort <- function(x) {
  XML::parseURI(x)$port
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

.MavenSolr <- setRefClass("MavenSolr",
                          fields = list(
                            pom = "character",
                            uri = "character"
                            ),
                          methods = list(
                            start = function() {
                              if (.self$isRunning()) {
                                warning("server already running (port is open)")
                                return()
                              }
                              log <- tempfile("solrlog")
                              cmd <- paste("mvn -f", pom,
                                           "jetty:run 2>&1 >", log, "&")
                              system(cmd)
                              message("Solr starting; if this takes more ",
                                      "than a few minutes, please see log: ",
                                      log)
                              port <- uriPort(uri)
                              while(!portIsOpen(port)) {
                                Sys.sleep(0.1)
                              }
                              message("Solr started at: ", .self$uri)
                            },
                            kill = function() {
                              if (.self$isRunning()) {
                                system(paste("mvn -f", pom,
                                             "jetty:stop 2>&1 >/dev/null"))
                              } else {
                                warning("Test solr not running")
                              }
                            },
                            isRunning = function() {
                              portIsOpen(uriPort(uri))
                            },
                            finalize = function() {
                              if (.self$isRunning())
                                .self$kill()
                            }))

MavenSolr <- function(start = TRUE)
{
  pom <- system.file("solr", "pom.xml", package="rsolr")
  uri <- "http://localhost:8983/solr"
  solr <- .MavenSolr$new(pom = pom, uri = uri)
  if (start) {
    solr$start()
  }
  solr
}

getSolrHome <- function() {
  file.path(tempdir(), "solr")
}

populateSolrHome <- function() {
  solr.home <- getSolrHome()
  file.copy(system.file("example-solr", "solr", package="rsolr"),
            dirname(solr.home), recursive=TRUE)
  solr.home
}

getStartJar <- function() {
  system.file("example-solr", "start.jar", package="rsolr")
}

buildCommandLine <- function() {
  paste("cd", dirname(getStartJar()), "; java",
        paste0("-Dsolr.solr.home=", getSolrHome()),
        paste0("-DSTOP.PORT=", 8079L),
        paste0("-DSTOP.KEY=", "rsolr"),
        "-jar", basename(getStartJar()))
}

.ExampleSolr <-
  setRefClass("ExampleSolr",
              fields = list(
                uri = "character"
                ),
              methods = list(
                start = function() {
                  if (.self$isRunning()) {
                    warning("server already running (port is open)")
                    return()
                  }
                  solr.home <- populateSolrHome()
                  cmd <- buildCommandLine()
                  system(paste(cmd, "&"))
                  port <- uriPort(uri)
                  while(!portIsOpen(port)) {
                    Sys.sleep(0.1)
                  }
                  message("Solr started at: ", .self$uri)
                },
                kill = function() {
                  if (.self$isRunning()) {
                    unlink(getSolrHome(), recursive=TRUE)
                    system(paste(buildCommandLine(), "--stop"))
                  } else {
                    warning("Test solr not running")
                  }
                },
                isRunning = function() {
                  portIsOpen(uriPort(uri))
                },
                finalize = function() {
                  if (.self$isRunning())
                    .self$kill()
                }))

TestSolr <- function(start = TRUE)
{
  uri <- "http://localhost:8983/solr"
  solr <- .ExampleSolr$new(uri = uri)
  if (start) {
    solr$start()
  }
  solr
}
