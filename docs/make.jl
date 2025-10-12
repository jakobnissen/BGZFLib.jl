using Documenter, BGZFLib

meta = quote
    using MemoryViews
    using BufferIO
    using LibDeflate
    using BGZFLib

    (path_to_bgzf, bgzf_data, path_to_gzi, gzi_data) = let
        dir = joinpath(Base.pkgdir(BGZFLib), "data")
        path_to_bgzf = joinpath(dir, "1.gz")
        bgzf_data = open(read, path_to_bgzf)
        path_to_gzi = joinpath(dir, "1.gzi")
        gzi_data = open(read, path_to_gzi)
        (path_to_bgzf, bgzf_data, path_to_gzi, gzi_data)
    end
end

DocMeta.setdocmeta!(BGZFLib, :DocTestSetup, meta; recursive = true)

makedocs(;
    sitename = "BGZFLib.jl",
    modules = [BGZFLib],
    pages = [
        "BGZFLib" => "index.md",
        "Readers" => "readers.md",
        "Seeking" => "gzindex.md",
        "Reference" => "reference.md",
    ],
    authors = "Jakob Nybo Nissen",
    checkdocs = :public,
    remotes = nothing,
)

deploydocs(;
    repo = "github.com/BioJulia/BGZFLib.jl.git",
    push_preview = true,
    deps = nothing,
    make = nothing,
)
