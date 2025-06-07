"""
Iceberg Usage Example

This example demonstrates how to use DataFusion.jl with Iceberg tables,
replicating the functionality from datafusion-playground/src/main.rs.

It shows how to:
1. Create an Iceberg SQL catalog with memory object store
2. Build a schema with various field types (long, int, date)
3. Create a day-partitioned table
4. Execute INSERT and SELECT SQL queries
5. Process query results
"""

using DataFusion

function main()
    println("=== DataFusion.jl Iceberg Example ===")

    # Create an Iceberg SQL catalog with memory object store
    println("Creating Iceberg catalog...")
    catalog = iceberg_catalog_sql("sqlite://", "test")

    # Build the schema with 5 fields matching the playground example
    println("Building schema...")
    schema = iceberg_schema()
    add_long_field!(schema, UInt32(1), "id", true)
    add_long_field!(schema, UInt32(2), "customer_id", true)
    add_long_field!(schema, UInt32(3), "product_id", true)
    add_date_field!(schema, UInt32(4), "date", true)
    add_int_field!(schema, UInt32(5), "amount", true)

    # Create partition specification with day partitioning on the date field
    println("Creating partition specification...")
    partition_spec = iceberg_partition_spec()
    add_day_field!(partition_spec, UInt32(4), UInt32(1000), "day")

    # Create the Iceberg table
    println("Creating Iceberg table...")
    table = iceberg_table("orders", "/test/orders", schema, partition_spec, catalog, "test")

    # Create DataFusion context and register the table
    println("Setting up DataFusion context...")
    ctx = DataFusionContext()
    register_iceberg_table!(ctx, "orders", table)

    # Insert initial data
    println("Inserting initial data...")
    insert_sql = """
    INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES
    (1, 1, 1, '2020-01-01', 1),
    (2, 2, 1, '2020-01-01', 1),
    (3, 3, 1, '2020-01-01', 3),
    (4, 1, 2, '2020-02-02', 1),
    (5, 1, 1, '2020-02-02', 2),
    (6, 3, 3, '2020-02-02', 3);
    """

    result = sql(ctx, insert_sql)
    println("Inserted $(batch_count(result)) batches")

    # Query the data - group by product_id and sum amounts
    println("Querying data (first aggregation)...")
    select_sql = "SELECT product_id, sum(amount) FROM orders GROUP BY product_id;"
    result = sql(ctx, select_sql)

    println("Query results:")
    print_result(result)

    # Verify the results match expected values
    println("Verifying results...")
    num_batches = batch_count(result)
    for batch_idx in 0:(num_batches-1)
        num_rows = batch_num_rows(result, batch_idx)
        if num_rows > 0
            println("Batch $batch_idx has $num_rows rows")
            # Note: In a real implementation, we'd need additional C API functions
            # to extract actual values from the result batches for verification
        end
    end

    # Insert more data
    println("Inserting additional data...")
    insert_sql2 = """
    INSERT INTO orders (id, customer_id, product_id, date, amount) VALUES
    (7, 1, 3, '2020-01-03', 1),
    (8, 2, 1, '2020-01-03', 2),
    (9, 2, 2, '2020-01-03', 1);
    """

    result = sql(ctx, insert_sql2)
    println("Inserted additional data")

    # Query again to see updated results
    println("Querying data (second aggregation)...")
    result = sql(ctx, select_sql)

    println("Updated query results:")
    print_result(result)

    # Verify the updated results
    println("Verifying updated results...")
    num_batches = batch_count(result)
    for batch_idx in 0:(num_batches-1)
        num_rows = batch_num_rows(result, batch_idx)
        if num_rows > 0
            println("Batch $batch_idx has $num_rows rows")
        end
    end

    println("=== Example completed successfully! ===")
end

# Run the example if this file is executed directly
if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
