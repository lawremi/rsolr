.undertowVersion <- "1.5.0-RC1"
.solrVersion <- "5.3.0"

.test <- function() {
    solr <- TestSolr()
    on.exit(solr$kill())
    get("testPackage", getNamespace("BiocGenerics"))("rsolr")
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
  if (s$socket == 0L) {
      FALSE
  } else {
      close.socket(s)
      TRUE
  }
}

solrIsReady <- function(uri) {
    schema <- try(read(RestUri(uri)$schema), silent=TRUE)
    !is(schema, "try-error")
}

getSolrHome <- function() {
  file.path(tempdir(), "solr")
}

getSolrLogs <- function() {
    file.path(tempdir(), "solr-logs")
}

populateSolrHome <- function(customSchema=NULL) {
  solr.home <- getSolrHome()
  unlink(solr.home, recursive=TRUE)  
  file.copy(system.file("solr", package="rsolr"),
            dirname(solr.home), recursive=TRUE)
  if (!is.null(customSchema)) {
      sampleCorePath <- file.path(solr.home, "cores", "techproducts")
      corePath <- file.path(solr.home, "cores", name(customSchema))
      file.rename(sampleCorePath, corePath)
      saveXML(customSchema, file.path(corePath, "conf", "schema.xml"))
      writeLines(paste0("name=", name(customSchema)),
                 file.path(corePath, "core.properties"))
      unlink(file.path(corePath, "data"), recursive=TRUE)
  }
  solr.home
}

getDownloadURL <- function() {
    paste0("https://github.com/bremeld/solr-undertow/releases/download/",
           "v", .undertowVersion, "/solr-undertow-", .undertowVersion,
           "-with-solr-", .solrVersion, ".zip")
}

downloadSolr <- function() {
    url <- getDownloadURL()
    dest <- tempfile()
### FIXME: drop curl method with R 3.2.2
    if (download.file(url, dest, method="curl", extra='-L') != 0L) {
        stop("download of Solr (undertow) failed")
    }
    dest
}

unpack <- function(file, exdir) {
    unzip(file, exdir=exdir)
}

maybeInstallSolr <- function() {
    if (!interactive()) {
        installSolr()
    } else {
        message("Install solr?")
        res <- readline("y/n: ")
        if (res == "y") {
            installSolr()
        } else {
            stop("Please install Solr to run tests/examples")
        }
    }
}

installSolr <- function() {
    message("Solr installation not found, installing...")
    file <- downloadSolr()
    unpack(file, getInstallPath())
}

getSolrPath <- function() {
    subdir <- file_path_sans_ext(basename(getDownloadURL()))
    file.path(getInstallPath(), subdir)
}

getInstallPath <- function() {
    path.expand(file.path(Sys.getenv("XDG_DATA_HOME", "~/.local/share"),
                          "R", "rsolr"))
}

solrIsInstalled <- function() {
     file.exists(getSolrPath())
}

getUndertowConfig <- function() {
    list(solr.undertow=
             list(solrHome=getSolrHome(),
                  solrLogs=getSolrLogs(),
                  tempDir=tempdir(),
                  solrVersion=.solrVersion,
                  solrWarFile=file.path(getSolrPath(), "example", "solr-wars",
                      paste0("solr-", .solrVersion, ".zip")),
                  shutdown=list(password="rsolr", gracefulDelay="0s")))
}

toHOCON <- function(x) {
    gsub("\"([A-z.]+)\":", "\\1:", x)
}

runSolr <- function() {
    if (!solrIsInstalled()) {
        message("Solr not found")
        maybeInstallSolr()
    }
    config <- getUndertowConfig()
    configPath <- tempfile("example-solr", fileext=".conf")
    writeLines(toHOCON(toJSON(config)), configPath)
    bin <- file.path(getSolrPath(), "bin", "solr-undertow")
    Sys.chmod(bin, "0755")
    system(paste(bin, configPath),
           wait=FALSE, ignore.stdout=!isTRUE(getOption("verbose")))
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
                    .self$kill()
                  }
                  populateSolrHome(.self$customSchema)
                  message("Starting Solr...")
                  message("Use options(verbose=TRUE) to diagnose any problems.")
                  runSolr()
                  while(!solrIsReady(.self$uri)) {
                      Sys.sleep(0.1)
                  }
                  if (!interactive())
                      Sys.sleep(Sys.getenv("RSOLR_TEST_START_SLEEP", 1L))
                  message("Solr started at: ", .self$uri)
                },
                kill = function() {
                  if (.self$isRunning()) {
                    unlink(getSolrHome(), recursive=TRUE)
                    read(RestUri("http://localhost:9983"), password="rsolr")
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
                }))

TestSolr <- function(schema = NULL, start = TRUE, restart=FALSE)
{
  uri <- file.path("http://localhost:8983/solr",
                   if (is.null(schema)) "techproducts" else name(schema))
  solr <- .ExampleSolr$new(uri = uri, customSchema = schema)
  if (start && (!solr$isRunning() || restart)) {
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
