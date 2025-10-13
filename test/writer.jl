function bgzfread(v::VecWriter)
    return SyncBGZFReader(read, CursorReader(v.vec))
end

@testset "From AbstractBufWriter" begin
    io = VecWriter()
    writer = BGZFWriter(io; n_workers = 2)
    @test writer isa BGZFWriter{VecWriter}
    @test write(writer, b"test data") == 9
    close(writer)

    @test String(bgzfread(io)) == "test data"
end

@testset "From IO" begin
    io = IOBuffer()
    writer = BGZFWriter(io; n_workers = 2)
    @test writer isa BGZFWriter{BufWriter{IOBuffer}}
    @test write(writer, b"test data") == 9
    flush(writer)
    seekstart(io)
    data = read(io)
    append!(data, BGZFLib.EOF_BLOCK)
    close(writer)

    reader = SyncBGZFReader(CursorReader(data))
    @test read(reader) == b"test data"
    close(reader)
end

@testset "Write and read back" begin
    @testset "Multiple writes" begin
        io = VecWriter()
        writer = BGZFWriter(io; n_workers = 2)
        write(writer, b"Hello, ")
        write(writer, b"world!")
        write(writer, b" More data.")
        close(writer)

        @test String(bgzfread(io)) == "Hello, world! More data."
    end

    @testset "Empty write" begin
        io = VecWriter()
        writer = BGZFWriter(io; n_workers = 2)
        close(writer)

        @test io.vec == BGZFLib.EOF_BLOCK
        @test String(bgzfread(io)) == ""
    end

    @testset "Large write" begin
        io = VecWriter()
        writer = BGZFWriter(io; n_workers = 4)
        data = repeat(b"0123456789", 10000)
        write(writer, data)
        close(writer)

        @test bgzfread(io) == data
    end
end

@testset "BGZFWriter - isopen and close" begin
    io = VecWriter()
    writer = BGZFWriter(io; n_workers = 2)

    @test isopen(writer)
    close(writer)
    @test !isopen(writer)
end

@testset "Single worker" begin
    io = VecWriter()
    writer = BGZFWriter(io; n_workers = 1)
    write(writer, b"single worker test")
    close(writer)

    @test String(bgzfread(io)) == "single worker test"
end

@testset "With append_empty=false" begin
    io = VecWriter()
    writer = BGZFWriter(io; n_workers = 2, append_empty = false)
    write(writer, b"test")
    close(writer)

    # Should error with check_truncated=true
    reader = SyncBGZFReader(CursorReader(io.vec); check_truncated = true)
    @test_throws BGZFError read(reader)
    close(reader)

    # Should work with check_truncated=false
    reader = SyncBGZFReader(CursorReader(io.vec); check_truncated = false)
    @test read(reader) == b"test"
    close(reader)
end

@testset "BGZFWriter - flush" begin
    @testset "Explicit flush" begin
        io = VecWriter()
        writer = BGZFWriter(io; n_workers = 2)

        write(writer, b"first part")
        flush(writer)
        write(writer, b" second part")
        close(writer)

        @test String(bgzfread(io)) == "first part second part"
    end

    @testset "Multiple flushes" begin
        io = VecWriter()
        writer = BGZFWriter(io; n_workers = 2)

        write(writer, b"one")
        flush(writer)
        write(writer, b"two")
        flush(writer)
        write(writer, b"three")
        close(writer)

        @test String(bgzfread(io)) == "onetwothree"
    end
end

@testset "BGZFWriter - write_empty_block" begin
    io = VecWriter()
    writer = BGZFWriter(io; n_workers = 2, append_empty = false)
    write(writer, b"before empty")
    write_empty_block(writer)
    close(writer)

    # The file should have an empty block and be readable with check_truncated=true
    reader = SyncBGZFReader(CursorReader(io.vec); check_truncated = true)
    @test read(reader) == b"before empty"
    close(reader)
end

@testset "BGZFWriter - function form" begin
    io = VecWriter()

    result = BGZFWriter(io; n_workers = 2) do writer
        write(writer, b"function form test")
    end

    @test result == 18
    @test String(bgzfread(io)) == "function form test"
end

@testset "Large write spanning multiple blocks" begin
    @testset "Single large write" begin
        io = VecWriter()
        writer = BGZFWriter(io; n_workers = 4)

        # Create data larger than SAFE_DECOMPRESSED_SIZE to span multiple blocks
        # SAFE_DECOMPRESSED_SIZE is 2^16 - 256 = 65280 bytes
        data = repeat(b"0123456789", 10000)  # 100000 bytes
        @test length(data) > 65280

        @test write(writer, data) == length(data)
        close(writer)

        reader = SyncBGZFReader(CursorReader(io.vec))
        @test read(reader) == data
        close(reader)
    end

    @testset "Multiple writes accumulating to large data" begin
        io = VecWriter()
        writer = BGZFWriter(io; n_workers = 2)

        chunk = repeat(b"abcdefghij", 1000)  # 10000 bytes per chunk
        total_written = 0

        for i in 1:10
            total_written += write(writer, chunk)
        end

        @test total_written == 100000
        close(writer)

        expected_data = repeat(chunk, 10)
        reader = SyncBGZFReader(CursorReader(io.vec))
        @test read(reader) == expected_data
        close(reader)
    end
end
