@testset "From AbstractBufWriter" begin
    io = VecWriter()
    writer = SyncBGZFWriter(io)
    @test writer isa SyncBGZFWriter{VecWriter}
    @test write(writer, b"test data") == 9
    close(writer)

    reader = BGZFReader(CursorReader(io.vec))
    @test read(reader) == b"test data"
    close(reader)
end

@testset "From IO" begin
    io = IOBuffer()
    writer = SyncBGZFWriter(io)
    @test writer isa SyncBGZFWriter{BufWriter{IOBuffer}}
    @test write(writer, b"test data") == 9
    flush(writer)
    seekstart(io)
    data = read(io)
    close(writer)
    reader = SyncBGZFReader(CursorReader(data))
    @test read(reader) == b"test data"
    close(reader)
end

@testset "Write and read back" begin
    @testset "Multiple writes" begin
        io = VecWriter()
        writer = SyncBGZFWriter(io)
        write(writer, b"Hello, ")
        write(writer, b"world!")
        write(writer, b" More data.")
        close(writer)

        reader = SyncBGZFReader(CursorReader(io.vec))
        @test read(reader) == b"Hello, world! More data."
        close(reader)
    end

    @testset "Empty write" begin
        io = VecWriter()
        writer = SyncBGZFWriter(io)
        close(writer)

        @test io.vec == BGZFLib.EOF_BLOCK
        reader = SyncBGZFReader(CursorReader(io.vec))
        @test read(reader) == UInt8[]
        close(reader)
    end
end
