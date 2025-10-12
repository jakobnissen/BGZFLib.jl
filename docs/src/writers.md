```@meta
DocTestSetup = quote
    using MemoryViews
    using BufferIO
    using LibDeflate
    using BGZFLib

    (path_to_bgzf, bgzf_data) = let
        dir = joinpath(Base.pkgdir(BGZFLib), "data")
        path_to_bgzf = joinpath(dir, "1.gz")
        bgzf_data = open(read, path_to_bgzf)
        (path_to_bgzf, bgzf_data)
    end
end
```

# BGZF writers
Like BGZF readers, there is a `BGZFWriter` and a `SyncBGZFWriter` with the same tradeoffs as the two readers. See the "readers" section in the sidebar.

### Constructing BGZF writers
Both BGZF writers conform to the [AbstractBufWriter interface](file:///home/jakni/code/BufferIO.jl/docs/build/writers/index.html).

The buffers of the BGZF writers are fixed in size. Calling `BufferIO.grow_buffer` on them will perform a shallow flush instead of expanding the buffer.

The `SyncBGZFWriter` wraps an existing `AbstractBufWriter`. This inner writer must be able to present a buffer of at least size 2^16, else a `BGZFError(nothing, BGZFErrors.insufficient_writer_space)` will be thrown.

When creating a `SyncBGZFWriter` from an `T <: IO`, a `SyncBGZFWriter{BufWriter{T}}` is created. Since `BufWriter` has an expanding buffer, it can always accomodate 2^16 bytes.

Mutating the wrapped io object of a BGZF reader or writer is not permitted and can cause erratic behaviour.

### Writing BGZF files
Writers have an `append_empty` keyword that defaults to `true`. If set to `true`, closing the BGZF writer will write an empty BGZF, signaling EOF.

Should you want to write an empty block in the middle of the stream, ehe function `write_empty_block` can be used:

```@docs; canonical=false
write_empty_block
```

Similar to BGZF readers (and `Base.open`), you can pass a function as the first argument to apply the function to the reader, then automatically close it:

```jldoctest
io = VecWriter()

# Write data to `io` in BGZF format
BGZFWriter(io) do writer
    write(writer, "Hello, world!")
end

# Now read it back
SyncBGZFReader(CursorReader(io.vec)) do reader
    read(reader, String)
end

# output
"Hello, world!"
```

## Reference
```@docs; canonical=false
BGZFWriter
SyncBGZFWriter
```