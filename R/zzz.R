.onLoad <- function(libname, pkgname) {
  populate_XDG_DATA_HOME()
}

populate_XDG_DATA_HOME <- function() {
  if (nchar(Sys.getenv("XDG_DATA_HOME")) == 0L) {
    Sys.setenv(XDG_DATA_HOME=path.expand("~/.local/share"))
  }
}
