globalVariables(c(".", ".field"))

.onLoad <- function(libname, pkgname) {
  populate_XDG_DATA_HOME()
}

populate_XDG_DATA_HOME <- function() {
  if (nchar(Sys.getenv("XDG_DATA_HOME")) == 0L) {
    Sys.setenv(XDG_DATA_HOME=path.expand("~/.local/share"))
  }
}

### FIXME: remove this hack after R is fixed (extends w/ class union bug)
local({
    .Expression <- getClass("Expression")
    .Expression@subclasses <-
        .Expression@subclasses[unique(names(.Expression@subclasses))]
    assignClassDef("Expression", .Expression, where = topenv())
})
