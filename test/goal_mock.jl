using Gradines

#=
    This is a mockup to give a very rough idea of what I'd like to be able
    to achieve with this package.
=#

const filename = "sometestfile.whatever"

# writing to gradines from some source
function input(datasource)
    g = Gradine(filename)

    for s ∈ datasource
        data = format(s)  # must go out of scope to free memory
        g = vcat(g, data)
    end

    g
end


# outputting machine learning arrays from gradines
function output(g::Gradine, ml)
    iter = train_iterate(g, size=256)

    for X, y ∈ iter
        train!(ml, X, y)
    end

    ml
end


# now we must do a test and plug back in
function putback(g::Gradine, ml)
    iter = test_iterate(g, size=256)

    for block ∈ iter
        append!(block, predict(ml, X))
    end

    g
end



