/*
* Copyright 2018-2020 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "agg_funcs.h"
#include "../../value.h"
#include "../../errors.h"
#include "../../util/arr.h"
#include "../../query_ctx.h"
#include "../../util/qsort.h"
#include "../../util/rmalloc.h"
#include "../../datatypes/array.h"
#include <math.h>
#include <float.h>

#define ISLT(a,b) ((*a) < (*b))

// Routine for freeing a generic aggregate function context.
void Aggregate_Free(void *ctx_ptr) {
	AggregateCtx *ctx = ctx_ptr;
	SIValue_Free(ctx->result);
	if(ctx->hashSet) Set_Free(ctx->hashSet);
	if(ctx->private_ctx) rm_free(ctx->private_ctx);
	rm_free(ctx);
}

// Routine for cloning a generic aggregate function context.
void *Aggregate_Clone(void *orig) {
	AggregateCtx *orig_ctx = orig;
	AggregateCtx *ctx_clone = rm_malloc(sizeof(AggregateCtx));
	ctx_clone->result = orig_ctx->result;
	ctx_clone->hashSet = orig_ctx->hashSet ? Set_New() : NULL;
	ctx_clone->private_ctx = NULL;
	return ctx_clone;
}

// Finalize the result of an aggregate function.
static inline void Aggregate_SetResult(AggregateCtx *ctx, SIValue result) {
	ctx->result = result;
}

void AGG_SUM(SIValue *argv, int argc) {
	AggregateCtx *ctx = argv[1].ptrval;
	// On the first invocation, initialize the context's value.
	if(SI_TYPE(ctx->result) == T_NULL) ctx->result = SI_DoubleVal(0);
	// convert the value of the input sequence to a double if possible
	SIValue v = argv[0];
	if(SI_TYPE(v) == T_NULL) return;
	// If we're uniquing inputs, return early if this value has already been seen.
	if(ctx->hashSet && Set_Add(ctx->hashSet, v) == false) return;

	// Update the total.
	if(SI_TYPE(v) != T_NULL) ctx->result.doubleval += SI_GET_NUMERIC(v);
}

//------------------------------------------------------------------------

typedef struct {
	size_t count;
	double total;
} _agg_AvgCtx;

void AGG_AVG(SIValue *argv, int argc) {
	AggregateCtx *ctx = argv[1].ptrval;
	// On the first invocation, initialize the context.
	if(ctx->private_ctx == NULL) ctx->private_ctx = rm_calloc(1, sizeof(_agg_AvgCtx));
	// convert the value of the input sequence to a double if possible
	_agg_AvgCtx *avg_ctx = ctx->private_ctx;
	SIValue v = argv[0];
	if(SI_TYPE(v) == T_NULL) return;
	// If we're uniquing inputs, return early if this value has already been seen.
	if(ctx->hashSet && Set_Add(ctx->hashSet, v) == false) return;
	avg_ctx->count ++;
	avg_ctx->total += SI_GET_NUMERIC(v);
}

void AvgFinalize(void *ctx_ptr) {
	AggregateCtx *ctx = ctx_ptr;
	_agg_AvgCtx *avg_ctx = ctx->private_ctx;
	if(avg_ctx->count > 0) Aggregate_SetResult(ctx, SI_DoubleVal(avg_ctx->total / avg_ctx->count));
	else Aggregate_SetResult(ctx, SI_DoubleVal(0));
}

/*

//------------------------------------------------------------------------

typedef struct {
    SIValue max;
    bool init;
} __agg_maxCtx;

int __agg_maxStep(AggCtx *ctx, SIValue *argv, int argc) {
    if(argc != 1) {
        // Emit error on invalid argument count.
        Error_IncorrectFunctionArgumentCount("Max", argc, 1);
        return AGG_ERR;
    }
    __agg_maxCtx *ac = Agg_FuncCtx(ctx);
    SIValue v = argv[0];
    // Any null values are excluded from the calculation.
    if(SIValue_IsNull(v)) return AGG_OK;

    if(!ac->init) {
        ac->init = true;
        ac->max = v;
        return AGG_OK;
    }

    if(SIValue_Compare(ac->max, v, NULL) < 0) {
        ac->max = v;
    }

    return AGG_OK;
}

int __agg_maxReduceNext(AggCtx *ctx) {
    __agg_maxCtx *ac = Agg_FuncCtx(ctx);
    Aggregate_SetResult(ctx, ac->max);
    return AGG_OK;
}

void __agg_maxCtxFree(AggCtx *ctx) {
    __agg_maxCtx *ac = Agg_FuncCtx(ctx);
    rm_free(ac);
}

void *__agg_maxCtxNew() {
    __agg_maxCtx *ac = rm_malloc(sizeof(__agg_maxCtx));
    ac->max = SI_NullVal();
    ac->init = false;
    return ac;
}

AggCtx *Agg_MaxFunc(bool distinct) {
    // Max aggregation do not care about distinct.
    return Agg_NewCtx(__agg_maxStep, __agg_maxReduceNext, __agg_maxCtxNew, __agg_maxCtxFree, distinct);
}

//------------------------------------------------------------------------

typedef struct {
    SIValue min;
    bool init;
} __agg_minCtx;

int __agg_minStep(AggCtx *ctx, SIValue *argv, int argc) {
    if(argc != 1) {
        // Emit error on invalid argument count.
        Error_IncorrectFunctionArgumentCount("Min", argc, 1);
        return AGG_ERR;
    }
    __agg_minCtx *ac = Agg_FuncCtx(ctx);
    SIValue v = argv[0];
    // Any null values are excluded from the calculation.
    if(SIValue_IsNull(v)) return AGG_OK;

    if(!ac->init) {
        ac->init = true;
        ac->min = v;
        return AGG_OK;
    }

    if(SIValue_Compare(ac->min, v, NULL) > 0) {
        ac->min = v;
    }

    return AGG_OK;
}

int __agg_minReduceNext(AggCtx *ctx) {
    __agg_minCtx *ac = Agg_FuncCtx(ctx);
    Aggregate_SetResult(ctx, ac->min);
    return AGG_OK;
}

void *__agg_minCtxNew() {
    __agg_minCtx *ac = rm_malloc(sizeof(__agg_minCtx));
    ac->min = SI_NullVal();
    ac->init = false;
    return ac;
}

void __agg_minCtxFree(AggCtx *ctx) {
    __agg_minCtx *ac = Agg_FuncCtx(ctx);
    rm_free(ac);
}

AggCtx *Agg_MinFunc(bool distinct) {
    // Min aggregation do not care about distinct.
    return Agg_NewCtx(__agg_minStep, __agg_minReduceNext, __agg_minCtxNew, __agg_minCtxFree, distinct);
}

//------------------------------------------------------------------------

typedef struct {
    size_t count;
    set *hashSet;
} __agg_countCtx;

int __agg_countStep(AggCtx *ctx, SIValue *argv, int argc) {
    if(argc != 1) {
        // Emit error on invalid argument count.
        Error_IncorrectFunctionArgumentCount("Count", argc, 1);
        return AGG_ERR;
    }
    __agg_countCtx *ac = Agg_FuncCtx(ctx);
    SIValue v = argv[0];
    // Batch size to this function is always one, so
    // we only need to check the first argument
    if(!SIValue_IsNullPtr(&v)) ac->count ++;

    return AGG_OK;
}

int __agg_countDistinctStep(AggCtx *ctx, SIValue *argv, int argc) {
    if(argc != 1) {
        // Emit error on invalid argument count.
        Error_IncorrectFunctionArgumentCount("Count", argc, 1);
        return AGG_ERR;
    }
    __agg_countCtx *ac = Agg_FuncCtx(ctx);
    SIValue v = argv[0];
    // If value not yet seen, process it with the original step method.
    if(Set_Add(ac->hashSet, v)) return __agg_countStep(ctx, argv, argc);
    return AGG_OK;
}

int __agg_countReduceNext(AggCtx *ctx) {
    __agg_countCtx *ac = Agg_FuncCtx(ctx);
    Aggregate_SetResult(ctx, SI_LongVal(ac->count));
    return AGG_OK;
}

void *__agg_countCtxNew(AggCtx *ctx) {
    __agg_countCtx *ac = rm_malloc(sizeof(__agg_countCtx));
    ac->count = 0;
    if(ctx->isDistinct) ac->hashSet = Set_New();
    else ac->hashSet = NULL;
    return ac;
}

void __agg_countCtxFree(AggCtx *ctx) {
    __agg_countCtx *ac = Agg_FuncCtx(ctx);
    if(ac->hashSet) Set_Free(ac->hashSet);
    rm_free(ac);
}

AggCtx *Agg_CountFunc(bool distinct) {
    if(distinct) {
        return Agg_NewCtx(__agg_countDistinctStep, __agg_countReduceNext, __agg_countCtxNew,
                          __agg_countCtxFree, distinct);
    } else {
        return Agg_NewCtx(__agg_countStep, __agg_countReduceNext, __agg_countCtxNew,
                          __agg_countCtxFree, distinct);
    }
}

//------------------------------------------------------------------------

typedef struct {
    double percentile;
    double *values;
    size_t count;
    size_t values_allocated;
    set *hashSet;
} __agg_percCtx;

// This function is agnostic as to percentile method
int __agg_percStep(AggCtx *ctx, SIValue *argv, int argc) {
    if(argc != 2) {
        // Emit error on invalid argument count.
        Error_IncorrectFunctionArgumentCount("Percentile", argc, 2);
        return AGG_ERR;
    }
    __agg_percCtx *ac = Agg_FuncCtx(ctx);

    // The last argument is the requested percentile, which we only
    // need to apply on the first function invocation (at which time
    // _agg_percCtx->percentile will be -1)
    if(ac->percentile < 0) {
        if(!SIValue_ToDouble(&argv[1], &ac->percentile)) {
            QueryCtx_SetError("PERC_DISC Could not convert percentile argument to double");
        }
        if(ac->percentile < 0 || ac->percentile > 1) {
            QueryCtx_SetError("PERC_DISC Invalid input for percentile is not a valid argument, must be a number in the range 0.0 to 1.0");
        }
    }

    if(ac->count > ac->values_allocated) {
        ac->values_allocated *= 2;
        ac->values = rm_realloc(ac->values, sizeof(double) * ac->values_allocated);
    }


    SIValue v = argv[0];
    SIType t = SI_TYPE(v);
    if(t == T_NULL) return AGG_OK;
    if(!(t & SI_NUMERIC)) {
        QueryCtx_SetError("PERC_DISC Could not convert upstream value to double");
        return AGG_ERR;
    }

    double n;
    SIValue_ToDouble(&v, &n);
    ac->values[ac->count++] = n;

    return AGG_OK;
}

int __agg_percDistinctStep(AggCtx *ctx, SIValue *argv, int argc) {
    if(argc != 2) {
        // Emit error on invalid argument count.
        Error_IncorrectFunctionArgumentCount("Percentile", argc, 2);
        return AGG_ERR;
    }
    __agg_percCtx *ac = Agg_FuncCtx(ctx);
    SIValue v = argv[0];
    // If value not yet seen, process it with the original step method.
    if(Set_Add(ac->hashSet, v)) return __agg_percStep(ctx, argv, argc);

    return AGG_OK;
}

int __agg_percDiscReduceNext(AggCtx *ctx) {
    __agg_percCtx *ac = Agg_FuncCtx(ctx);

    if(ac->count == 0) {
        Aggregate_SetResult(ctx, SI_NullVal());
        return AGG_OK;
    }

    QSORT(double, ac->values, ac->count, ISLT);

    // If ac->percentile == 0, employing this formula would give an index of -1
    int idx = ac->percentile > 0 ? ceil(ac->percentile * ac->count) - 1 : 0;
    double n = ac->values[idx];
    Aggregate_SetResult(ctx, SI_DoubleVal(n));

    return AGG_OK;
}

int __agg_percContReduceNext(AggCtx *ctx) {
    __agg_percCtx *ac = Agg_FuncCtx(ctx);

    QSORT(double, ac->values, ac->count, ISLT);

    if(ac->count == 0) {
        Aggregate_SetResult(ctx, SI_NullVal());
        return AGG_OK;
    }

    if(ac->percentile == 1.0 || ac->count == 1) {
        Aggregate_SetResult(ctx, SI_DoubleVal(ac->values[ac->count - 1]));
        return AGG_OK;
    }

    double int_val, fraction_val;
    double float_idx = ac->percentile * (ac->count - 1);
    // Split the temp value into its integer and fractional values
    fraction_val = modf(float_idx, &int_val);
    int index = int_val; // Casting the integral part of the value to an int for convenience

    if(!fraction_val) {
        // A valid index was requested, so we can directly return a value
        Aggregate_SetResult(ctx, SI_DoubleVal(ac->values[index]));
        return AGG_OK;
    }

    double lhs, rhs;
    lhs = ac->values[index] * (1 - fraction_val);
    rhs = ac->values[index + 1] * fraction_val;

    Aggregate_SetResult(ctx, SI_DoubleVal(lhs + rhs));

    return AGG_OK;
}

void *__agg_PercCtxNew(AggCtx *ctx) {
    __agg_percCtx *ac = rm_malloc(sizeof(__agg_percCtx));
    ac->count = 0;
    ac->values = rm_malloc(1024 * sizeof(double));
    ac->values_allocated = 1024;
    // Percentile will be updated by the first call to Step
    ac->percentile = -1;
    if(ctx->isDistinct) ac->hashSet = Set_New();
    else ac->hashSet = NULL;
    return ac;
}

void __agg_PercCtxFree(AggCtx *ctx) {
    __agg_percCtx *ac = Agg_FuncCtx(ctx);
    rm_free(ac->values);
    if(ac->hashSet) Set_Free(ac->hashSet);
    rm_free(ac);
}

// The percentile initializers are identical save for the ReduceNext function they specify
AggCtx *Agg_PercDiscFunc(bool distinct) {
    if(distinct) {
        return Agg_NewCtx(__agg_percDistinctStep, __agg_percDiscReduceNext, __agg_PercCtxNew,
                          __agg_PercCtxFree, distinct);
    } else {
        return Agg_NewCtx(__agg_percStep, __agg_percDiscReduceNext, __agg_PercCtxNew,
                          __agg_PercCtxFree, distinct);
    }
}

AggCtx *Agg_PercContFunc(bool distinct) {
    if(distinct) {
        return Agg_NewCtx(__agg_percDistinctStep, __agg_percContReduceNext, __agg_PercCtxNew,
                          __agg_PercCtxFree, distinct);
    } else {
        return Agg_NewCtx(__agg_percStep, __agg_percContReduceNext, __agg_PercCtxNew,
                          __agg_PercCtxFree, distinct);
    }
}

//------------------------------------------------------------------------

typedef struct {
    double *values;
    double total;
    size_t count;
    size_t values_allocated;
    int is_sampled;
    set *hashSet;
} __agg_stdevCtx;

int __agg_StdevStep(AggCtx *ctx, SIValue *argv, int argc) {
    if(argc != 1) {
        // Emit error on invalid argument count.
        Error_IncorrectFunctionArgumentCount("stDev", argc, 1);
        return AGG_ERR;
    }
    __agg_stdevCtx *ac = Agg_FuncCtx(ctx);

    if(ac->count + argc > ac->values_allocated) {
        ac->values_allocated *= 2;
        ac->values = rm_realloc(ac->values, sizeof(double) * ac->values_allocated);
    }


    SIValue v = argv[0];
    SIType t = SI_TYPE(v);
    if(t == T_NULL) return AGG_OK;
    if(!(t & SI_NUMERIC)) {
        QueryCtx_SetError("STDEV Could not convert upstream value to double");
        return AGG_ERR;
    }

    double n;
    SIValue_ToDouble(&v, &n);
    ac->values[ac->count++] = n;
    ac->total += n;

    return AGG_OK;
}

int __agg_StdevDistinctStep(AggCtx *ctx, SIValue *argv, int argc) {
    if(argc != 1) {
        // Emit error on invalid argument count.
        Error_IncorrectFunctionArgumentCount("stDev", argc, 1);
        return AGG_ERR;
    }
    __agg_stdevCtx *ac = Agg_FuncCtx(ctx);
    SIValue v = argv[0];
    // If value not yet seen, process it with the original step method.
    if(Set_Add(ac->hashSet, v)) return __agg_StdevStep(ctx, argv, argc);

    return AGG_OK;
}

int __agg_StdevReduceNext(AggCtx *ctx) {
    __agg_stdevCtx *ac = Agg_FuncCtx(ctx);

    if(ac->count < 2) {
        Aggregate_SetResult(ctx, SI_DoubleVal(0));
        return AGG_OK;
    }

    double mean = ac->total / ac->count;
    long double sum = 0;
    for(int i = 0; i < ac->count; i ++) {
        sum += (long double)(ac->values[i] - mean) * (ac->values[i] + mean);
    }
    // is_sampled will be equal to 1 in the Stdev case and 0 in the StdevP case
    double variance = sum / (ac->count - ac->is_sampled);
    double stdev = sqrt(variance);

    Aggregate_SetResult(ctx, SI_DoubleVal(stdev));

    return AGG_OK;
}

void *__agg_stdevCtxNew(AggCtx *ctx) {
    __agg_stdevCtx *ac = rm_malloc(sizeof(__agg_stdevCtx));
    ac->is_sampled = 1;
    ac->count = 0;
    ac->total = 0;
    ac->values = rm_malloc(1024 * sizeof(double));
    ac->values_allocated = 1024;
    if(ctx->isDistinct) ac->hashSet = Set_New();
    else ac->hashSet = NULL;
    return ac;
}

void __agg_StdevCtxFree(AggCtx *ctx) {
    __agg_stdevCtx *ac = Agg_FuncCtx(ctx);
    rm_free(ac->values);
    if(ac->hashSet) Set_Free(ac->hashSet);
    rm_free(ac);
}

AggCtx *Agg_StdevFunc(bool distinct) {
    if(distinct) {
        return Agg_NewCtx(__agg_StdevDistinctStep, __agg_StdevReduceNext, __agg_stdevCtxNew,
                          __agg_StdevCtxFree, distinct);
    } else {
        return Agg_NewCtx(__agg_StdevStep, __agg_StdevReduceNext, __agg_stdevCtxNew,
                          __agg_StdevCtxFree, distinct);
    }
}

// StdevP is identical to Stdev save for an altered value we can check for with a bool
AggCtx *Agg_StdevPFunc(bool distinct) {
    AggCtx *func = Agg_StdevFunc(distinct);
    __agg_stdevCtx *ac = Agg_FuncCtx(func);
    ac->is_sampled = 0;
    return func;
}

//------------------------------------------------------------------------

typedef struct {
    SIValue list;
    set *hashSet;
} __agg_collectCtx;

int __agg_collectStep(AggCtx *ctx, SIValue *argv, int argc) {
    // convert multiple values to array
    if(argc != 1) {
        // Emit error on invalid argument count.
        Error_IncorrectFunctionArgumentCount("Collect", argc, 1);
        return AGG_ERR;
    }

    __agg_collectCtx *ac = Agg_FuncCtx(ctx);

    SIValue value = argv[0];
    if(value.type == T_NULL) return AGG_OK;
    // SIArray_Append will clone the added value, ensuring it can be
    // safely accessed for the lifetime of the Collect context.
    SIArray_Append(&ac->list, value);
    return AGG_OK;
}

int __agg_collectDistinctStep(AggCtx *ctx, SIValue *argv, int argc) {
    if(argc != 1) {
        // Emit error on invalid argument count.
        Error_IncorrectFunctionArgumentCount("Collect", argc, 1);
        return AGG_ERR;
    }

    __agg_collectCtx *ac = Agg_FuncCtx(ctx);
    SIValue v = argv[0];
    // If value not yet seen, process it with the original step method.
    if(Set_Add(ac->hashSet, v)) return __agg_collectStep(ctx, argv, argc);
    return AGG_OK;
}

int __agg_collectReduceNext(AggCtx *ctx) {
    __agg_collectCtx *ac = Agg_FuncCtx(ctx);
    // Share the Collect context's internal list with the caller,
    // as the Collect context owns this allocation. The caller is responsible for
    // persisting the value if it will be accessed after the Collect context is freed.
    SIValue result = SI_ShareValue(ac->list);
    Aggregate_SetResult(ctx, result);
    return AGG_OK;
}

void *__agg_collectCtxNew(AggCtx *ctx) {
    __agg_collectCtx *ac = rm_malloc(sizeof(__agg_collectCtx));
    ac->list = SI_Array(100);
    if(ctx->isDistinct) ac->hashSet = Set_New();
    else ac->hashSet = NULL;
    return ac;
}

void __agg_collectCtxFree(AggCtx *ctx) {
    __agg_collectCtx *ac = Agg_FuncCtx(ctx);
    if(ac->hashSet) Set_Free(ac->hashSet);
    SIValue_Free(ac->list);
    rm_free(ac);
}

AggCtx *Agg_CollectFunc(bool distinct) {
    if(distinct) {
        return Agg_NewCtx(__agg_collectDistinctStep, __agg_collectReduceNext, __agg_collectCtxNew,
                          __agg_collectCtxFree, distinct);
    } else {
        return Agg_NewCtx(__agg_collectStep, __agg_collectReduceNext, __agg_collectCtxNew,
                          __agg_collectCtxFree, distinct);
    }
}
*/

//------------------------------------------------------------------------

void Register_AggFuncs() {
	SIType *types;
	AR_FuncDesc *func_desc;
	types = array_new(SIType, 1);
	types = array_append(types, T_NULL | T_INT64 | T_DOUBLE);
	types = array_append(types, T_PTR);
	func_desc = AR_FuncDescNew("sum", AGG_SUM, 2, 2, types, true, true);
	AR_SetPrivateDataRoutines(func_desc, Aggregate_Free, Aggregate_Clone);
	AR_RegFunc(func_desc);

	types = array_new(SIType, 1);
	types = array_append(types, T_NULL | T_INT64 | T_DOUBLE);
	types = array_append(types, T_PTR);
	func_desc = AR_FuncDescNew("avg", AGG_AVG, 2, 2, types, false, true);
	AR_SetPrivateDataRoutines(func_desc, Aggregate_Free, Aggregate_Clone);
	AR_SetFinalizeRoutine(func_desc, AvgFinalize);
	AR_RegFunc(func_desc);

	/*
	SIType *types;
	AR_FuncDesc *func_desc;
	types = array_new(SIType, 1);
	types = array_append(types, T_NULL | T_INT64 | T_DOUBLE);
	func_desc = AR_FuncDescNew("max", Agg_MaxFunc, 1, 1, types, true, false);
	AR_RegFunc(func_desc, true);
	Agg_RegisterFunc("max", Agg_MaxFunc);

	SIType *types;
	AR_FuncDesc *func_desc;
	types = array_new(SIType, 1);
	types = array_append(types, T_NULL | T_INT64 | T_DOUBLE);
	func_desc = AR_FuncDescNew("min", Agg_MinFunc, 1, 1, types, true, false);
	AR_RegFunc(func_desc, true);

	SIType *types;
	AR_FuncDesc *func_desc;
	types = array_new(SIType, 1);
	types = array_append(types, T_NULL | T_INT64 | T_DOUBLE);
	func_desc = AR_FuncDescNew("min", Agg_MinFunc, 1, 1, types, true, false);
	AR_RegFunc(func_desc, true);

	SIType *types;
	AR_FuncDesc *func_desc;
	types = array_new(SIType, 1);
	types = array_append(types, T_ANY);
	func_desc = AR_FuncDescNew("count", Agg_CountFunc, 1, 1, types, true, false);
	AR_RegFunc(func_desc, true);

	SIType *types;
	AR_FuncDesc *func_desc;
	types = array_new(SIType, 1);
	types = array_append(types, T_NULL | T_INT64 | T_DOUBLE);
	func_desc = AR_FuncDescNew("percentileDisc", Agg_PercDiscFunc, 1, 1, types, true, false);
	AR_RegFunc(func_desc, true);

	SIType *types;
	AR_FuncDesc *func_desc;
	types = array_new(SIType, 1);
	types = array_append(types, T_NULL | T_INT64 | T_DOUBLE);
	func_desc = AR_FuncDescNew("percentileCont", Agg_PercContFunc, 1, 1, types, true, false);
	AR_RegFunc(func_desc, true);

	SIType *types;
	AR_FuncDesc *func_desc;
	types = array_new(SIType, 1);
	types = array_append(types, T_NULL | T_INT64 | T_DOUBLE);
	func_desc = AR_FuncDescNew("stDev", Agg_StdevFunc, 1, 1, types, true, false);
	AR_RegFunc(func_desc, true);

	SIType *types;
	AR_FuncDesc *func_desc;
	types = array_new(SIType, 1);
	types = array_append(types, T_NULL | T_INT64 | T_DOUBLE);
	func_desc = AR_FuncDescNew("stDevP", Agg_StdevPFunc, 1, 1, types, true, false);
	AR_RegFunc(func_desc, true);

	SIType *types;
	AR_FuncDesc *func_desc;
	types = array_new(SIType, 1);
	types = array_append(types, T_ANY);
	func_desc = AR_FuncDescNew("collect", Agg_CollectFunc, 1, 1, types, true, false);
	AR_RegFunc(func_desc, true);
	*/
}

bool Aggregate_PerformsDistinct(AggregateCtx *ctx) {
	return (ctx->hashSet != NULL);
}

SIValue Aggregate_GetResult(AggregateCtx *ctx) {
	return SI_ShareValue(ctx->result);
}

