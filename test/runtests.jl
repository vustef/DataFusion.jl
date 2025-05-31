using Test
using DataFusion

@testset "DataFusion.jl Tests" begin

    # Create test data
    test_csv = "test_employees.csv"
    test_data = """id,name,age,department,salary
1,Alice,25,Engineering,75000
2,Bob,30,Marketing,65000
3,Carol,35,Engineering,85000"""

    open(test_csv, "w") do f
        write(f, test_data)
    end

    try
        @testset "Context Creation" begin
            ctx = DataFusionContext()
            @test ctx isa DataFusionContext
            @test ctx.ptr != C_NULL
        end

        @testset "CSV Registration and Query Execution" begin
            ctx = DataFusionContext()

            # Test CSV registration
            @test_nowarn register_csv!(ctx, "employees", test_csv)

            # Test basic query
            result = sql(ctx, "SELECT * FROM employees")
            @test result isa DataFusionResult
            @test result.ptr != C_NULL

            # Test result metadata
            @test batch_count(result) >= 1
            @test batch_num_rows(result, 0) == 3
            @test batch_num_columns(result, 0) == 5
        end

        @testset "SQL Queries" begin
            ctx = DataFusionContext()
            register_csv!(ctx, "employees", test_csv)

            # Test filtering
            result = sql(ctx, "SELECT name FROM employees WHERE age > 30")
            @test batch_num_rows(result, 0) == 1  # Only Carol

            # Test aggregation
            result = sql(ctx, "SELECT COUNT(*) as total FROM employees")
            @test batch_num_rows(result, 0) == 1
            @test batch_num_columns(result, 0) == 1
        end

        @testset "Error Handling" begin
            ctx = DataFusionContext()

            # Test invalid CSV file
            @test_throws Exception register_csv!(ctx, "invalid", "nonexistent.csv")

            # Test invalid SQL
            register_csv!(ctx, "employees", test_csv)
            @test_throws Exception sql(ctx, "SELECT * FROM nonexistent_table")
        end

    finally
        # Cleanup
        if isfile(test_csv)
            rm(test_csv)
        end
    end
end
