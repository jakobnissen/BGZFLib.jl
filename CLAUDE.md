# Overall instructions
* Avoid comments where the purpose can easily be inferred from the code and variable names themselves

# How to write tests
* Keep tests small and self-contained. Do not make long chains of tests that mutate the same state, unless it's necessary for the test logic.
* See existing tests for the style and structure of the tests

# Docstrings for BufferIO functions
These should be followed by implementations in BGZFLib.
In particular, readers and writers in BGZFLib cannot expand their buffer.

## fill_buffer
"""
    fill_buffer(io::AbstractBufReader)::Union{Int, Nothing}

Fill more bytes into the buffer from `io`'s underlying buffer, returning
the number of bytes added. After calling `fill_buffer` and getting `n`,
the buffer obtained by `get_buffer` should have `n` new bytes appended.

This function must fill at least one byte, except
* If the underlying io is EOF, or there is no underlying io to fill bytes from, return 0
* If the buffer is not empty, and cannot be expanded, return `nothing`.

Buffered readers which do not wrap another underlying IO, and therefore can't fill
its buffer should return 0 unconditionally.
This function should never return `nothing` if the buffer is empty.

!!! note
    Idiomatically, users should not call `fill_buffer` when the buffer is not empty,
    because doing so may force growing the buffer instead of letting `io` choose an optimal
    buffer size. Calling `fill_buffer` with a nonempty buffer is only appropriate if, for
    algorithmic reasons you need `io` itself to buffer some minimum amount of data.
"""

# get_buffer
"""
    get_buffer(io::AbstractBufReader)::ImmutableMemoryView{UInt8}

Get the available bytes of `io`.

Calling this function, even when the buffer is empty, should never do actual system I/O,
and in particular should not attempt to fill the buffer.
To fill the buffer, call [`fill_buffer`](@ref).

    get_buffer(io::AbstractBufWriter)::MutableMemoryView{UInt8}

Get the available mutable buffer of `io` that can be written to.

Calling this function should never do actual system I/O, and in particular
should not attempt to flush data from the buffer or grow the buffer.
To increase the size of the buffer, call [`grow_buffer`](@ref).
"""

# consume
"""
    consume(io::Union{AbstractBufReader, AbstractBufWriter}, n::Int)::Nothing

Remove the first `n` bytes of the buffer of `io`.
Consumed bytes will not be returned by future calls to `get_buffer`.

If n is negative, or larger than the current buffer size,
throw an `IOError` with `ConsumeBufferError` kind.
This check is a boundscheck and may be elided with `@inbounds`.
"""
