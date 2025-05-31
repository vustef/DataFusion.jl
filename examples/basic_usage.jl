#!/usr/bin/env julia

"""
Basic DataFusion.jl Usage Example

This example demonstrates how to:
1. Create a DataFusion context
2. Register CSV files as tables
3. Execute SQL queries
4. Inspect and print results
"""

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using DataFusion

function main()
    println("DataFusion.jl Basic Usage Example")
    println("=" ^ 40)

    # Create a sample CSV file for demonstration
    sample_data = """id,name,age,department,salary
1,Alice,25,Engineering,75000
2,Bob,30,Marketing,65000
3,Carol,35,Engineering,85000
4,David,28,Sales,55000
5,Eve,32,Engineering,80000
6,Frank,45,Marketing,70000
7,Grace,29,Sales,58000
8,Henry,38,Engineering,90000"""

    csv_path = "employees.csv"
    open(csv_path, "w") do f
        write(f, sample_data)
    end
    println("Created sample CSV file: $csv_path")

    try
        # Create a DataFusion context
        println("\n1. Creating DataFusion context...")
        ctx = DataFusionContext()
        println("✓ Context created successfully")

        # Register the CSV file
        println("\n2. Registering CSV file as table 'employees'...")
        register_csv!(ctx, "employees", csv_path)
        println("✓ CSV registered successfully")

        # Execute some queries
        queries = [
            ("Basic SELECT", "SELECT * FROM employees"),
            ("Filtered query", "SELECT name, age, salary FROM employees WHERE age > 30"),
            ("Aggregation", "SELECT department, COUNT(*) as count, AVG(salary) as avg_salary FROM employees GROUP BY department"),
            ("Sorting", "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 3")
        ]

        for (description, query) in queries
            println("\n3. Executing query: $description")
            println("Query: $query")
            println("-" ^ 50)

            result = sql(ctx, query)
            print_result(result)

            # Show result metadata
            println("\nResult metadata:")
            batches = batch_count(result)
            println("  - Number of batches: $batches")

            if batches > 0
                rows = batch_num_rows(result, 0)
                cols = batch_num_columns(result, 0)
                println("  - First batch: $rows rows, $cols columns")
            end
        end

        println("\n✓ All queries executed successfully!")

    catch e
        println("Error: $e")
        rethrow(e)
    finally
        # Clean up the sample file
        if isfile(csv_path)
            rm(csv_path)
            println("\nCleaned up sample file: $csv_path")
        end
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
