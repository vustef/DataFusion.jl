"""
    DataFusion

Julia bindings for Apache Arrow DataFusion, providing a high-performance SQL query engine.
"""
module DataFusion

using Libdl

export DataFusionContext, DataFusionResult,
       register_csv!, sql, print_result,
       batch_count, batch_num_rows, batch_num_columns

# Load the native library
const LIBRARY_PATH = Ref{String}()

function __init__()
    # Try to find the library in common locations
    possible_paths = [
        "./libdatafusion_c_api.dylib",  # macOS, local build
        "./libdatafusion_c_api.so",    # Linux, local build
        "./datafusion_c_api.dll",      # Windows, local build
        "../datafusion-c-api/target/release/libdatafusion_c_api.dylib",  # Relative to Julia project
        "../datafusion-c-api/target/release/libdatafusion_c_api.so",
        "../datafusion-c-api/target/release/datafusion_c_api.dll",
        "../datafusion-c-api/target/debug/libdatafusion_c_api.dylib",
        "../datafusion-c-api/target/debug/libdatafusion_c_api.so",
        "../datafusion-c-api/target/debug/datafusion_c_api.dll",
    ]

    LIBRARY_PATH[] = ""
    for path in possible_paths
        if isfile(path)
            LIBRARY_PATH[] = path
            break
        end
    end

    if LIBRARY_PATH[] == ""
        error("Could not find DataFusion C library. Please build the datafusion-c-api project first.")
    end
end

# Error codes
const DATAFUSION_OK = 0
const DATAFUSION_ERROR = -1

# Opaque pointers
const DataFusionContextPtr = Ptr{Cvoid}
const DataFusionResultPtr = Ptr{Cvoid}

"""
    DataFusionContext

A DataFusion execution context that manages query execution and data sources.
"""
mutable struct DataFusionContext
    ptr::DataFusionContextPtr

    function DataFusionContext()
        ptr = ccall((:datafusion_context_new, LIBRARY_PATH[]), DataFusionContextPtr, ())
        if ptr == C_NULL
            error("Failed to create DataFusion context")
        end

        ctx = new(ptr)
        finalizer(ctx) do c
            if c.ptr != C_NULL
                ccall((:datafusion_context_free, LIBRARY_PATH[]), Cvoid, (DataFusionContextPtr,), c.ptr)
                c.ptr = C_NULL
            end
        end

        return ctx
    end
end

"""
    DataFusionResult

A result from executing a SQL query, containing one or more record batches.
"""
mutable struct DataFusionResult
    ptr::DataFusionResultPtr

    function DataFusionResult(ptr::DataFusionResultPtr)
        if ptr == C_NULL
            error("Invalid result pointer")
        end

        result = new(ptr)
        finalizer(result) do r
            if r.ptr != C_NULL
                ccall((:datafusion_result_free, LIBRARY_PATH[]), Cvoid, (DataFusionResultPtr,), r.ptr)
                r.ptr = C_NULL
            end
        end

        return result
    end
end

"""
    register_csv!(ctx::DataFusionContext, table_name::String, file_path::String)

Register a CSV file as a table in the DataFusion context.

# Arguments
- `ctx`: The DataFusion context
- `table_name`: Name to assign to the table
- `file_path`: Path to the CSV file

# Examples
```julia
ctx = DataFusionContext()
register_csv!(ctx, "employees", "employees.csv")
```
"""
function register_csv!(ctx::DataFusionContext, table_name::String, file_path::String)
    if ctx.ptr == C_NULL
        error("DataFusion context has been freed")
    end

    result = ccall((:datafusion_register_csv, LIBRARY_PATH[]), Cint,
                   (DataFusionContextPtr, Cstring, Cstring),
                   ctx.ptr, table_name, file_path)

    if result != DATAFUSION_OK
        error("Failed to register CSV file: $(get_last_error())")
    end

    return nothing
end

"""
    sql(ctx::DataFusionContext, query::String) -> DataFusionResult

Execute a SQL query and return the results.

# Arguments
- `ctx`: The DataFusion context
- `query`: SQL query string

# Examples
```julia
ctx = DataFusionContext()
register_csv!(ctx, "employees", "employees.csv")
result = sql(ctx, "SELECT * FROM employees WHERE age > 30")
```
"""
function sql(ctx::DataFusionContext, query::String)
    if ctx.ptr == C_NULL
        error("DataFusion context has been freed")
    end

    result_ptr = ccall((:datafusion_sql, LIBRARY_PATH[]), DataFusionResultPtr,
                       (DataFusionContextPtr, Cstring),
                       ctx.ptr, query)

    if result_ptr == C_NULL
        error("Failed to execute SQL query: $(get_last_error())")
    end

    return DataFusionResult(result_ptr)
end

"""
    print_result(result::DataFusionResult)

Print the result as a formatted table.

# Arguments
- `result`: The DataFusion query result

# Examples
```julia
result = sql(ctx, "SELECT * FROM employees")
print_result(result)
```
"""
function print_result(result::DataFusionResult)
    if result.ptr == C_NULL
        error("DataFusion result has been freed")
    end

    ret = ccall((:datafusion_result_print, LIBRARY_PATH[]), Cint,
                (DataFusionResultPtr,), result.ptr)

    if ret != DATAFUSION_OK
        error("Failed to print result: $(get_last_error())")
    end

    return nothing
end

"""
    batch_count(result::DataFusionResult) -> Int

Get the number of record batches in the result.

# Arguments
- `result`: The DataFusion query result

# Returns
The number of record batches
"""
function batch_count(result::DataFusionResult)
    if result.ptr == C_NULL
        error("DataFusion result has been freed")
    end

    return ccall((:datafusion_result_batch_count, LIBRARY_PATH[]), Cint,
                 (DataFusionResultPtr,), result.ptr)
end

"""
    batch_num_rows(result::DataFusionResult, batch_index::Int) -> Int

Get the number of rows in a specific batch.

# Arguments
- `result`: The DataFusion query result
- `batch_index`: Zero-based index of the batch

# Returns
The number of rows in the specified batch
"""
function batch_num_rows(result::DataFusionResult, batch_index::Int)
    if result.ptr == C_NULL
        error("DataFusion result has been freed")
    end

    return ccall((:datafusion_result_batch_num_rows, LIBRARY_PATH[]), Cint,
                 (DataFusionResultPtr, Cint), result.ptr, batch_index)
end

"""
    batch_num_columns(result::DataFusionResult, batch_index::Int) -> Int

Get the number of columns in a specific batch.

# Arguments
- `result`: The DataFusion query result
- `batch_index`: Zero-based index of the batch

# Returns
The number of columns in the specified batch
"""
function batch_num_columns(result::DataFusionResult, batch_index::Int)
    if result.ptr == C_NULL
        error("DataFusion result has been freed")
    end

    return ccall((:datafusion_result_batch_num_columns, LIBRARY_PATH[]), Cint,
                 (DataFusionResultPtr, Cint), result.ptr, batch_index)
end

"""
    get_last_error() -> String

Get the last error message from the DataFusion C library.
"""
function get_last_error()
    error_ptr = ccall((:datafusion_get_last_error, LIBRARY_PATH[]), Cstring, ())
    return unsafe_string(error_ptr)
end

end # module DataFusion
