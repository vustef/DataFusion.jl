"""
    DataFusion

Julia bindings for Apache Arrow DataFusion, providing a high-performance SQL query engine.
"""
module DataFusion

export DataFusionContext, DataFusionResult,
       register_csv!, sql, print_result,
       batch_count, batch_num_rows, batch_num_columns,
       # Iceberg exports
       IcebergCatalog, IcebergSchema, IcebergPartitionSpec, IcebergTable,
       iceberg_catalog_sql, iceberg_schema, add_long_field!, add_int_field!, add_date_field!,
       iceberg_partition_spec, add_day_field!, iceberg_table, register_iceberg_table!

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
        "../../datafusion-c-api/target/release/libdatafusion_c_api.dylib",  # From test directory
        "../../datafusion-c-api/target/release/libdatafusion_c_api.so",
        "../../datafusion-c-api/target/release/datafusion_c_api.dll",
        "../../datafusion-c-api/target/debug/libdatafusion_c_api.dylib",
        "../../datafusion-c-api/target/debug/libdatafusion_c_api.so",
        "../../datafusion-c-api/target/debug/datafusion_c_api.dll",
    ]

    println("DataFusion.__init__: Current working directory: ", pwd())
    LIBRARY_PATH[] = ""
    for path in possible_paths
        println("DataFusion.__init__: Checking path: ", path, " - ", isfile(path) ? "EXISTS" : "NOT FOUND")
        if isfile(path)
            LIBRARY_PATH[] = path
            println("DataFusion.__init__: Found library at: ", path)
            break
        end
    end

    if LIBRARY_PATH[] == ""
        println("DataFusion.__init__: No library found in any of the searched paths")
        error("Could not find DataFusion C library. Please build the datafusion-c-api project first.")
    else
        println("DataFusion.__init__: Using library: ", LIBRARY_PATH[])
    end
end

# Error codes
const DATAFUSION_OK = 0

# Opaque pointers
const DataFusionContextPtr = Ptr{Cvoid}
const DataFusionResultPtr = Ptr{Cvoid}
const IcebergCatalogPtr = Ptr{Cvoid}
const IcebergSchemaPtr = Ptr{Cvoid}
const IcebergPartitionSpecPtr = Ptr{Cvoid}
const IcebergTablePtr = Ptr{Cvoid}

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

"""
    IcebergCatalog

An Iceberg catalog for managing table metadata.
"""
mutable struct IcebergCatalog
    ptr::IcebergCatalogPtr

    function IcebergCatalog(database_url::String, name::String)
        ptr = ccall((:iceberg_catalog_new_sql, LIBRARY_PATH[]), IcebergCatalogPtr,
                   (Cstring, Cstring), database_url, name)
        if ptr == C_NULL
            error("Failed to create Iceberg catalog")
        end

        catalog = new(ptr)
        finalizer(catalog) do c
            if c.ptr != C_NULL
                ccall((:iceberg_catalog_free, LIBRARY_PATH[]), Cvoid, (IcebergCatalogPtr,), c.ptr)
                c.ptr = C_NULL
            end
        end

        return catalog
    end
end

"""
    IcebergSchema

An Iceberg schema builder for defining table structure.
"""
mutable struct IcebergSchema
    ptr::IcebergSchemaPtr

    function IcebergSchema()
        ptr = ccall((:iceberg_schema_new, LIBRARY_PATH[]), IcebergSchemaPtr, ())
        if ptr == C_NULL
            error("Failed to create Iceberg schema")
        end

        schema = new(ptr)
        finalizer(schema) do s
            if s.ptr != C_NULL
                ccall((:iceberg_schema_free, LIBRARY_PATH[]), Cvoid, (IcebergSchemaPtr,), s.ptr)
                s.ptr = C_NULL
            end
        end

        return schema
    end
end

"""
    IcebergPartitionSpec

An Iceberg partition specification for defining table partitioning.
"""
mutable struct IcebergPartitionSpec
    ptr::IcebergPartitionSpecPtr

    function IcebergPartitionSpec()
        ptr = ccall((:iceberg_partition_spec_new, LIBRARY_PATH[]), IcebergPartitionSpecPtr, ())
        if ptr == C_NULL
            error("Failed to create Iceberg partition spec")
        end

        spec = new(ptr)
        finalizer(spec) do s
            if s.ptr != C_NULL
                ccall((:iceberg_partition_spec_free, LIBRARY_PATH[]), Cvoid, (IcebergPartitionSpecPtr,), s.ptr)
                s.ptr = C_NULL
            end
        end

        return spec
    end
end

"""
    IcebergTable

An Iceberg table instance.
"""
mutable struct IcebergTable
    ptr::IcebergTablePtr

    function IcebergTable(ptr::IcebergTablePtr)
        if ptr == C_NULL
            error("Invalid Iceberg table pointer")
        end

        table = new(ptr)
        finalizer(table) do t
            if t.ptr != C_NULL
                ccall((:iceberg_table_free, LIBRARY_PATH[]), Cvoid, (IcebergTablePtr,), t.ptr)
                t.ptr = C_NULL
            end
        end

        return table
    end
end

"""
    iceberg_catalog_sql(database_url::String, name::String) -> IcebergCatalog

Create a new SQL-based Iceberg catalog.

# Arguments
- `database_url`: Database connection URL (e.g., "sqlite://")
- `name`: Catalog name

# Examples
```julia
catalog = iceberg_catalog_sql("sqlite://", "test")
```
"""
function iceberg_catalog_sql(database_url::String, name::String)
    return IcebergCatalog(database_url, name)
end

"""
    iceberg_schema() -> IcebergSchema

Create a new Iceberg schema builder.

# Examples
```julia
schema = iceberg_schema()
add_long_field!(schema, 1, "id", true)
add_long_field!(schema, 2, "customer_id", true)
```
"""
function iceberg_schema()
    return IcebergSchema()
end

"""
    add_long_field!(schema::IcebergSchema, id::UInt32, name::String, required::Bool)

Add a long (64-bit integer) field to the schema.

# Arguments
- `schema`: The Iceberg schema
- `id`: Field ID
- `name`: Field name
- `required`: Whether the field is required

# Examples
```julia
schema = iceberg_schema()
add_long_field!(schema, 1, "id", true)
```
"""
function add_long_field!(schema::IcebergSchema, id::UInt32, name::String, required::Bool)
    if schema.ptr == C_NULL
        error("Iceberg schema has been freed")
    end

    result = ccall((:iceberg_schema_add_long_field, LIBRARY_PATH[]), Bool,
                   (IcebergSchemaPtr, UInt32, Cstring, Bool),
                   schema.ptr, id, name, required)

    if !result
        error("Failed to add long field to schema")
    end

    return nothing
end

"""
    add_int_field!(schema::IcebergSchema, id::UInt32, name::String, required::Bool)

Add an int (32-bit integer) field to the schema.

# Arguments
- `schema`: The Iceberg schema
- `id`: Field ID
- `name`: Field name
- `required`: Whether the field is required

# Examples
```julia
schema = iceberg_schema()
add_int_field!(schema, 5, "amount", true)
```
"""
function add_int_field!(schema::IcebergSchema, id::UInt32, name::String, required::Bool)
    if schema.ptr == C_NULL
        error("Iceberg schema has been freed")
    end

    result = ccall((:iceberg_schema_add_int_field, LIBRARY_PATH[]), Bool,
                   (IcebergSchemaPtr, UInt32, Cstring, Bool),
                   schema.ptr, id, name, required)

    if !result
        error("Failed to add int field to schema")
    end

    return nothing
end

"""
    add_date_field!(schema::IcebergSchema, id::UInt32, name::String, required::Bool)

Add a date field to the schema.

# Arguments
- `schema`: The Iceberg schema
- `id`: Field ID
- `name`: Field name
- `required`: Whether the field is required

# Examples
```julia
schema = iceberg_schema()
add_date_field!(schema, 4, "date", true)
```
"""
function add_date_field!(schema::IcebergSchema, id::UInt32, name::String, required::Bool)
    if schema.ptr == C_NULL
        error("Iceberg schema has been freed")
    end

    result = ccall((:iceberg_schema_add_date_field, LIBRARY_PATH[]), Bool,
                   (IcebergSchemaPtr, UInt32, Cstring, Bool),
                   schema.ptr, id, name, required)

    if !result
        error("Failed to add date field to schema")
    end

    return nothing
end

"""
    iceberg_partition_spec() -> IcebergPartitionSpec

Create a new Iceberg partition specification.

# Examples
```julia
spec = iceberg_partition_spec()
add_day_field!(spec, 4, 1000, "day")
```
"""
function iceberg_partition_spec()
    return IcebergPartitionSpec()
end

"""
    add_day_field!(spec::IcebergPartitionSpec, source_id::UInt32, field_id::UInt32, name::String)

Add a day partition field to the partition specification.

# Arguments
- `spec`: The Iceberg partition spec
- `source_id`: Source field ID
- `field_id`: Partition field ID
- `name`: Partition field name

# Examples
```julia
spec = iceberg_partition_spec()
add_day_field!(spec, 4, 1000, "day")
```
"""
function add_day_field!(spec::IcebergPartitionSpec, source_id::UInt32, field_id::UInt32, name::String)
    if spec.ptr == C_NULL
        error("Iceberg partition spec has been freed")
    end

    result = ccall((:iceberg_partition_spec_add_day_field, LIBRARY_PATH[]), Bool,
                   (IcebergPartitionSpecPtr, UInt32, UInt32, Cstring),
                   spec.ptr, source_id, field_id, name)

    if !result
        error("Failed to add day field to partition spec")
    end

    return nothing
end

"""
    iceberg_table(name::String, location::String, schema::IcebergSchema,
                  partition_spec::IcebergPartitionSpec, catalog::IcebergCatalog,
                  namespace_name::String) -> IcebergTable

Create a new Iceberg table.

# Arguments
- `name`: Table name
- `location`: Table location
- `schema`: Table schema
- `partition_spec`: Partition specification
- `catalog`: Iceberg catalog
- `namespace_name`: Namespace name

# Examples
```julia
catalog = iceberg_catalog_sql("sqlite://", "test")
schema = iceberg_schema()
add_long_field!(schema, 1, "id", true)
spec = iceberg_partition_spec()
table = iceberg_table("orders", "/test/orders", schema, spec, catalog, "test")
```
"""
function iceberg_table(name::String, location::String, schema::IcebergSchema,
                      partition_spec::IcebergPartitionSpec, catalog::IcebergCatalog,
                      namespace_name::String)
    if schema.ptr == C_NULL
        error("Iceberg schema has been freed")
    end
    if partition_spec.ptr == C_NULL
        error("Iceberg partition spec has been freed")
    end
    if catalog.ptr == C_NULL
        error("Iceberg catalog has been freed")
    end

    ptr = ccall((:iceberg_table_create, LIBRARY_PATH[]), IcebergTablePtr,
                (Cstring, Cstring, IcebergSchemaPtr, IcebergPartitionSpecPtr, IcebergCatalogPtr, Cstring),
                name, location, schema.ptr, partition_spec.ptr, catalog.ptr, namespace_name)

    if ptr == C_NULL
        error("Failed to create Iceberg table")
    end

    # IMPORTANT: Mark the schema and partition_spec as consumed to prevent double-free
    # The C API takes ownership of these objects when creating the table
    schema.ptr = C_NULL
    partition_spec.ptr = C_NULL

    return IcebergTable(ptr)
end

"""
    register_iceberg_table!(ctx::DataFusionContext, table_name::String, table::IcebergTable)

Register an Iceberg table with the DataFusion context.

# Arguments
- `ctx`: The DataFusion context
- `table_name`: Name to assign to the table
- `table`: The Iceberg table

# Examples
```julia
ctx = DataFusionContext()
table = iceberg_table("orders", "/test/orders", schema, spec, catalog, "test")
register_iceberg_table!(ctx, "orders", table)
```
"""
function register_iceberg_table!(ctx::DataFusionContext, table_name::String, table::IcebergTable)
    if ctx.ptr == C_NULL
        error("DataFusion context has been freed")
    end
    if table.ptr == C_NULL
        error("Iceberg table has been freed")
    end

    result = ccall((:datafusion_register_iceberg_table, LIBRARY_PATH[]), Cint,
                   (DataFusionContextPtr, Cstring, IcebergTablePtr),
                   ctx.ptr, table_name, table.ptr)

    if result != DATAFUSION_OK
        error("Failed to register Iceberg table: $(get_last_error())")
    end

    return nothing
end

end # module DataFusion
