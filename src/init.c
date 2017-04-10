#include <Rinternals.h>
#include <R_ext/Rdynload.h>

SEXP top_prenv(SEXP nm, SEXP env);
SEXP top_prenv_dots(SEXP env);

#define CALLDEF(name, n)  {#name, (DL_FUNC) &name, n}

static R_CallMethodDef CallEntries[] = {
    CALLDEF(top_prenv, 2),
    CALLDEF(top_prenv_dots, 1),
    {NULL, NULL, 0}
};


void R_init_restfulr(DllInfo *dll)
{
    // Register C routines
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
