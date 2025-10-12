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

# BGZF readers
### `BGZFReader` and `SyncBGZFReader`
BGZFLib exposes two readers:

* `BGZFReader` uses a number of worker tasks to decompress concurrently with reading. This gives the highest performance and should be the default choice. However, in my experience Julia's task scheduler is not fantastic and can cause performance problems.
* `SyncBGZFReader` avoids task scheduling by decompressing in the reading task. Therefore, it neither decompresses in parallel, nor concurrent with reading, which makes it slower. However, it is simpler, less likely to have bugs, and does not strain the scheduler.

Most users should use `BGZFReader`.

### Constructing BGZF readers
Both readers make use of the [AbstractBufReader interface](file:///home/jakni/code/BufferIO.jl/docs/build/readers/index.html).
They both contain un-expandable buffers, i.e. `fill_buffer` generally cannot expand the buffer size and will return `nothing` if the buffer is not empty.

Readers wrap another `AbstractBufReader` containing the compressed BGZF stream:

```jldoctest
using BufferIO
using BGZFLib

reader = CursorReader(bgzf_data);
@assert reader isa AbstractBufReader
bgzf_reader = BGZFReader(reader)
println(String(read(bgzf_reader))) 
close(bgzf_reader)

# output
Hello, world!more dataxthen some moremore content herethis is another block
```

The `AbstractBufReader` must be able to buffer a full BGZF block, or the remainder of the underlying file, whichever is smallest.

If an `AbstractBufReader` with less space is provided, a `BGZFError(nothing, BGZFErrors.insufficient_reader_space)` is thrown.

If a reader is constructed from an `IO`, it is automatically wrapped in a `BufReader` with an appropriate buffer size:

```jldoctest
bgzf_reader = SyncBGZFReader(IOBuffer(bgzf_data))
println(String(read(bgzf_reader))) 
close(bgzf_reader)

# output
Hello, world!more dataxthen some moremore content herethis is another block
```

Mutating the wrapped io object of a BGZF reader or writer is not permitted and can cause erratic behaviour.

### Reading BGZF files
Readers have a `check_truncated` keyword that defaults to `true`. If set to `true`, the reader will error if the last block in the stream is not empty, marking EOF:

Example with `check_truncated = true` (default)
```jldoctest
reader = CursorReader(bgzf_data[1:end-28])
String(read(BGZFReader(reader)))

# output
ERROR: BGZFError(0, BGZFLib.BGZFErrors.truncated_file)
[...]
```

And set to `false`
```jldoctest
reader = CursorReader(bgzf_data[1:end-28])
String(read(BGZFReader(reader; check_truncated = false)))

# output
"Hello, world!more dataxthen some moremore content herethis is another block"
```

Like `Base.open`, the BGZF readers also ahave a method that takes a function as a first argument, and makes sure to close the reader even if it errors:

```jldoctest
SyncBGZFReader(io -> String(read(io)), CursorReader(bgzf_data))

# output
"Hello, world!more dataxthen some moremore content herethis is another block"
```

## Errors and error recovery
Types in this package generally throw `BGZFError`s:

```@docs; canonical=false
BGZFError
BGZFErrors
```

However, some operations on BGZF readers and writers propagate to their underlying IO, which may throw different errors.
For example, when calling `seek` on a BGZF reader wrapping a file (e.g. `SyncBGZFReader{BufReader{IOStream}}`), `seek` is also called on the underlying `IOStream`. This may throw another error.

When attempting to read a malformed BGZF file, the reader will throw a `BGZFError` and be in an error state. In this state, some operations like `BufferIO.fill_buffer` and `Base.seek` will throw a `BGZFError(nothing, BGZFErrors.operation_on_error)`.

To recover the BGZF reader, `seek` to a valid position:

```jldoctest
bad_data = append!(copy(bgzf_data), "some bad data")
reader = BGZFReader(CursorReader(bad_data))

# Trigger an error from reading bad gzip data
try
    read(reader)
catch error
    @assert error isa BGZFError
    @assert error.type isa LibDeflateError
end

# Trying to read from a reader in an error state will throw
# a BGZF error.
# Note that e.g. calling `read` would throw the same error
try
    fill_buffer(reader)
catch error
    @assert error isa BGZFError
    @assert error.type === BGZFErrors.operation_on_error
end

# Reset the reader by calling `seek`. If seek succeeds, the error
# state will disappear.
seekstart(reader)
println(String(read(reader, 13)))
close(reader)

# output
Hello, world!

```

## Reference
```@docs; canonical=false
BGZFReader
SyncBGZFReader
```