globalVariables(c(".", ".field"))

### FIXME: remove this hack after R is fixed (extends w/ class union bug)
local({
    .Expression <- getClass("Expression")
    .Expression@subclasses <-
        .Expression@subclasses[unique(names(.Expression@subclasses))]
    assignClassDef("Expression", .Expression, where = topenv())
})
