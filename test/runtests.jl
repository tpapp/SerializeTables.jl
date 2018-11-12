using SerializeTables, Test, Tables, CodecZlib

"Randomly return `missing` instead if the argument 1% of the time, otherwise the argument."
randmissing(x) = rand() < 0.01 ? missing : x

@testset "write and read small dataset" begin
    tmp = tempname()
    table = collect((a = i, b = Float64(i), c = 'a' + (i - 1)) for i in 1:10)
    serialize_table_rows(tmp, table)
    rowsitr = deserialize_table_rows(tmp)
    @test Tables.schema(rowsitr) == Tables.schema(Tables.rows(table))
    @test rowtable(rowsitr) == table
end

@testset "write and read large dataset w/ missing data" begin
    N = 10^6
    a = randmissing.(rand(rand(Float64, 100), N))
    b = randmissing.(rand(1:10000, N))
    tmp = tempname()
    table = columntable((a = a, b = b))
    serialize_table_rows(GzipCompressor(; level = 1), tmp, table)
    @info("compression",
          rat1 = filesize(tmp) / (N*(2+sizeof(Int)+sizeof(Float64))), # crude, but fast
          rat2 = filesize(tmp) / (Base.summarysize(a) + Base.summarysize(b))) # slow
    rowsitr = deserialize_table_rows(GzipDecompressor(), tmp)
    @test Tables.schema(rowsitr) == Tables.schema(table)
    @test all(rowtable(rowsitr) .≡ rowtable(table))
end
