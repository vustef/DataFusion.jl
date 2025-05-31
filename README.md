# DataFusion.jl

Julia bindings for [Apache Arrow DataFusion](https://datafusion.apache.org/), a fast, embeddable, modular analytic query engine for SQL queries.

## Features

- **High Performance**: Leverages the speed and efficiency of DataFusion's Rust implementation
- **SQL Support**: Full SQL query capabilities with DataFusion's comprehensive dialect
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

### Working with Multiple Tables

```julia
using DataFusion

ctx = DataFusionContext()

# Register multiple CSV files
register_csv!(ctx, "employees", "employees.csv")
register_csv!(ctx, "departments", "departments.csv")

# Join tables
result = sql(ctx, """
    SELECT e.name, e.salary, d.department_name 
    FROM employees e 
    JOIN departments d ON e.department_id = d.id
    ORDER BY e.salary DESC
""")
print_result(result)
```

### Advanced Analytics

```julia
using DataFusion

ctx = DataFusionContext()
register_csv!(ctx, "sales", "sales_data.csv")

# Window functions
result = sql(ctx, """
    SELECT 
        product,
        sale_date,
        amount,
        SUM(amount) OVER (PARTITION BY product ORDER BY sale_date) as running_total
    FROM sales
    ORDER BY product, sale_date
""")
print_result(result)

# Complex aggregations
result = sql(ctx, """
    SELECT 
        EXTRACT(YEAR FROM sale_date) as year,
        EXTRACT(MONTH FROM sale_date) as month,
        SUM(amount) as monthly_sales,
        COUNT(*) as num_transactions
    FROM sales
    GROUP BY EXTRACT(YEAR FROM sale_date), EXTRACT(MONTH FROM sale_date)
    ORDER BY year, month
""")
print_result(result)
```

## Running Examples

The package includes several example scripts:

```bash
# Run the basic usage example
cd DataFusion.jl
julia --project=. examples/basic_usage.jl
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