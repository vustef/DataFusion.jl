# DataFusion.jl

Julia bindings for [Apache Arrow DataFusion](https://datafusion.apache.org/), a fast, embeddable, modular analytic query engine for SQL queries.

## Features

- **High Performance**: Leverages the speed and efficiency of DataFusion's Rust implementation
- **SQL Support**: Full SQL query capabilities with DataFusion's comprehensive dialect
- **Iceberg Support**: Full Apache Iceberg table format support with partitioning and schema evolution (powered by [iceberg-rust](https://github.com/JanKaul/iceberg-rust))
- **Memory Safe**: Automatic resource management with Julia's garbage collector
- **Easy Integration**: Simple API for registering data sources and executing queries
- **Cross Platform**: Works on macOS, Linux, and Windows

## Prerequisites

1. **Julia 1.8+**: This package requires Julia 1.8 or later
2. **Rust 1.70+**: Required to build the C API
3. **Git**: For cloning the repositories

## Installation

DataFusion.jl requires the companion [datafusion-c-api](https://github.com/vustef/datafusion-c-api) C library. Follow these steps to set up both components:

### Step 1: Clone the Repositories

```bash
# Clone both repositories
git clone https://github.com/vustef/datafusion-c-api.git
git clone https://github.com/vustef/DataFusion.jl.git
```

### Step 2: Build the C API

```bash
cd datafusion-c-api
cargo build --release
```

This will create the necessary dynamic library (`libdatafusion_c_api.dylib` on macOS, `libdatafusion_c_api.so` on Linux, or `datafusion_c_api.dll` on Windows) in `target/release/`.

### Step 3: Set Up Julia Package

```bash
cd ../DataFusion.jl
julia --project=. -e "using Pkg; Pkg.instantiate()"
```

### Step 4: Verify Installation

```bash
julia --project=. examples/basic_usage.jl
```

## Directory Structure

Your setup should look like this:
```
your-workspace/
├── datafusion-c-api/          # C API repository
│   ├── src/
│   ├── target/release/        # Built library location
│   └── ...
└── DataFusion.jl/             # Julia package repository
    ├── src/
    ├── examples/
    └── ...
```

The Julia package automatically looks for the C library in the relative path `../datafusion-c-api/target/release/`.

## Quick Start

```julia
using DataFusion

# Create a DataFusion context
ctx = DataFusionContext()

# Register a CSV file as a table
register_csv!(ctx, "employees", "employees.csv")

# Execute SQL queries
result = sql(ctx, "SELECT * FROM employees WHERE age > 30")

# Print the results
print_result(result)

# Get result metadata
println("Batches: ", batch_count(result))
println("Rows in first batch: ", batch_num_rows(result, 0))
println("Columns in first batch: ", batch_num_columns(result, 0))
```

### Iceberg Tables

DataFusion.jl supports Apache Iceberg tables with full schema and partitioning capabilities:

```julia
using DataFusion

# Create an Iceberg catalog
catalog = iceberg_catalog_sql("sqlite://", "my_catalog")

# Build a schema
schema = iceberg_schema()
add_long_field!(schema, UInt32(1), "id", true)
add_long_field!(schema, UInt32(2), "customer_id", true)
add_date_field!(schema, UInt32(3), "order_date", true)
add_int_field!(schema, UInt32(4), "amount", true)

# Create partition specification
partition_spec = iceberg_partition_spec()
add_day_field!(partition_spec, UInt32(3), UInt32(1000), "day")

# Create and register Iceberg table
table = iceberg_table("orders", "/path/to/orders", schema, partition_spec, catalog, "my_catalog")
ctx = DataFusionContext()
register_iceberg_table!(ctx, "orders", table)

# Use the table with SQL
result = sql(ctx, "INSERT INTO orders VALUES (1, 100, '2024-01-01', 250)")
result = sql(ctx, "SELECT * FROM orders WHERE order_date >= '2024-01-01'")
print_result(result)
```

## API Reference

### Types

#### `DataFusionContext`
Represents a DataFusion execution context that manages query execution and data sources.

```julia
ctx = DataFusionContext()
```

#### `DataFusionResult`
Represents the result of a SQL query execution, containing one or more record batches.

### Functions

#### `register_csv!(ctx::DataFusionContext, table_name::String, file_path::String)`
Register a CSV file as a table in the DataFusion context.

**Arguments:**
- `ctx`: The DataFusion context
- `table_name`: Name to assign to the table
- `file_path`: Path to the CSV file

**Example:**
```julia
register_csv!(ctx, "sales_data", "/path/to/sales.csv")
```

#### Iceberg Functions

##### `iceberg_catalog_sql(database_url::String, name::String) -> IcebergCatalog`
Create a new SQL-based Iceberg catalog.

**Arguments:**
- `database_url`: Database connection URL (e.g., "sqlite://")
- `name`: Catalog name

**Example:**
```julia
catalog = iceberg_catalog_sql("sqlite://", "my_catalog")
```

##### `iceberg_schema() -> IcebergSchema`
Create a new Iceberg schema builder.

**Example:**
```julia
schema = iceberg_schema()
add_long_field!(schema, UInt32(1), "id", true)
add_date_field!(schema, UInt32(2), "created_at", true)
```

##### `add_long_field!(schema::IcebergSchema, id::UInt32, name::String, required::Bool)`
Add a long (64-bit integer) field to the schema.

##### `add_int_field!(schema::IcebergSchema, id::UInt32, name::String, required::Bool)`
Add an int (32-bit integer) field to the schema.

##### `add_date_field!(schema::IcebergSchema, id::UInt32, name::String, required::Bool)`
Add a date field to the schema.

##### `iceberg_partition_spec() -> IcebergPartitionSpec`
Create a new Iceberg partition specification.

##### `add_day_field!(spec::IcebergPartitionSpec, source_id::UInt32, field_id::UInt32, name::String)`
Add day-based partitioning to the partition specification.

##### `iceberg_table(name::String, location::String, schema::IcebergSchema, partition_spec::IcebergPartitionSpec, catalog::IcebergCatalog, catalog_name::String) -> IcebergTable`
Create a new Iceberg table.

##### `register_iceberg_table!(ctx::DataFusionContext, table_name::String, table::IcebergTable)`
Register an Iceberg table with the DataFusion context.

#### `sql(ctx::DataFusionContext, query::String) -> DataFusionResult`
Execute a SQL query and return the results.

**Arguments:**
- `ctx`: The DataFusion context
- `query`: SQL query string

**Returns:** A `DataFusionResult` containing the query results

**Example:**
```julia
result = sql(ctx, "SELECT product, SUM(amount) FROM sales_data GROUP BY product")
```

#### `print_result(result::DataFusionResult)`
Print the result as a formatted table to stdout.

**Arguments:**
- `result`: The DataFusion query result

#### `batch_count(result::DataFusionResult) -> Int`
Get the number of record batches in the result.

#### `batch_num_rows(result::DataFusionResult, batch_index::Int) -> Int`
Get the number of rows in a specific batch.

#### `batch_num_columns(result::DataFusionResult, batch_index::Int) -> Int`
Get the number of columns in a specific batch.

## Examples

### Basic Query Operations

```julia
using DataFusion

# Create context and register data
ctx = DataFusionContext()
register_csv!(ctx, "employees", "employees.csv")

# Basic SELECT
result = sql(ctx, "SELECT name, age FROM employees")
print_result(result)

# Filtering
result = sql(ctx, "SELECT * FROM employees WHERE department = 'Engineering'")
print_result(result)

# Aggregation
result = sql(ctx, "SELECT department, COUNT(*) as count, AVG(salary) as avg_salary 
                   FROM employees 
                   GROUP BY department")
print_result(result)

# Sorting and limiting
result = sql(ctx, "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 5")
print_result(result)
```

### Working with Iceberg Tables

```julia
using DataFusion

# Create an Iceberg catalog and schema
catalog = iceberg_catalog_sql("sqlite://", "analytics")
schema = iceberg_schema()
add_long_field!(schema, UInt32(1), "id", true)
add_long_field!(schema, UInt32(2), "customer_id", true)
add_long_field!(schema, UInt32(3), "product_id", true)
add_date_field!(schema, UInt32(4), "order_date", true)
add_int_field!(schema, UInt32(5), "amount", true)

# Create day-partitioned table
partition_spec = iceberg_partition_spec()
add_day_field!(partition_spec, UInt32(4), UInt32(1000), "day")

table = iceberg_table("orders", "/data/orders", schema, partition_spec, catalog, "analytics")

# Register with DataFusion
ctx = DataFusionContext()
register_iceberg_table!(ctx, "orders", table)

# Insert data
sql(ctx, """
    INSERT INTO orders (id, customer_id, product_id, order_date, amount) VALUES
    (1, 101, 1, '2024-01-01', 250),
    (2, 102, 2, '2024-01-01', 150),
    (3, 103, 1, '2024-01-02', 300)
""")

# Query with time-based filtering (takes advantage of partitioning)
result = sql(ctx, """
    SELECT product_id, SUM(amount) as total_sales
    FROM orders 
    WHERE order_date >= '2024-01-01' AND order_date < '2024-01-02'
    GROUP BY product_id
""")
print_result(result)
```

## Running Examples

The package includes several example scripts:

```bash
# Run the basic usage example
cd DataFusion.jl
julia --project=. examples/basic_usage.jl

# Run the Iceberg example
julia --project=. examples/iceberg_usage.jl
```

## Testing

Run the test suite to verify everything is working:

```bash
cd DataFusion.jl
julia --project=. test/runtests.jl
```

## Error Handling

DataFusion.jl provides informative error messages for common issues:

```julia
try
    ctx = DataFusionContext()
    register_csv!(ctx, "nonexistent", "missing_file.csv")
catch e
    println("Error: ", e)
    # Handle the error appropriately
end
```

## Performance Tips

1. **Use Release Build**: Always build the C API in release mode for best performance
2. **Batch Operations**: DataFusion is optimized for batch processing - avoid row-by-row operations
3. **Predicate Pushdown**: Use WHERE clauses to filter data early in the query
4. **Column Selection**: Only select the columns you need to reduce memory usage

## Troubleshooting

### "Could not find DataFusion C library"
This error occurs when the Julia package cannot locate the compiled Rust library. Ensure that:
1. You've built the `datafusion-c-api` project with `cargo build --release`
2. The library file exists in `../datafusion-c-api/target/release/`
3. Both repositories are in the same parent directory

### Build Issues
If you encounter build errors:
1. Ensure you have Rust 1.70+ installed: `rustc --version`
2. Update your Rust installation: `rustup update`
3. Clean and rebuild: `cargo clean && cargo build --release`

### Memory Issues
If you encounter memory-related errors:
1. Ensure you're not holding references to freed contexts or results
2. Julia's garbage collector should handle memory cleanup automatically
3. Check that the C library was built without memory issues

### Performance Issues
If queries are running slowly:
1. Verify you're using the release build of the C library
2. Check that your CSV files are properly formatted
3. Consider using more specific queries with appropriate filtering

## Architecture

DataFusion.jl consists of two main components:

1. **[datafusion-c-api](https://github.com/vustef/datafusion-c-api)**: A Rust crate that provides C-compatible bindings to Apache Arrow DataFusion
2. **DataFusion.jl**: This Julia package that provides high-level Julia bindings to the C API

The architecture provides:
- **Safety**: Memory management handled by both Rust and Julia garbage collectors
- **Performance**: Direct calls to DataFusion's optimized Rust implementation
- **Compatibility**: Standard C ABI ensures broad compatibility across platforms

## Related Projects

- **[Apache Arrow DataFusion](https://datafusion.apache.org/)** - The underlying query engine
- **[iceberg-rust](https://github.com/JanKaul/iceberg-rust)** - Rust implementation of Apache Iceberg with DataFusion integration (used for Iceberg support)
- **[datafusion-c-api](https://github.com/vustef/datafusion-c-api)** - C bindings for DataFusion (required dependency)
- **[Arrow.jl](https://github.com/apache/arrow-julia)** - Julia bindings for Apache Arrow
- **[DataFrames.jl](https://github.com/JuliaData/DataFrames.jl)** - DataFrames implementation in Julia

## Contributing

Contributions are welcome! Please ensure that:
1. The Rust C API builds successfully
2. All Julia examples run without errors
3. New features include appropriate documentation and examples
4. Tests pass: `julia --project=. test/runtests.jl`

When contributing, you may need to make changes to both repositories:
- **C API changes**: Submit PRs to [datafusion-c-api](https://github.com/vustef/datafusion-c-api)
- **Julia binding changes**: Submit PRs to this repository

## License

This project is licensed under the same terms as Apache Arrow DataFusion - Apache License 2.0. 