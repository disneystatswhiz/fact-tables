using DataFrames, Parquet, TimeZones

ENV["S3_FACT_URI"] = get(ENV, "S3_FACT_URI",
    "s3://touringplans_stats/stats_work/fact_tables/wait_time_fact_table.parquet")

run(`aws s3 cp $(ENV["S3_FACT_URI"]) ./work/fact_table/current_fact.parquet --only-show-errors`)

df = DataFrame(Parquet.read_parquet("./work/fact_table/current_fact.parquet"))
first(df, 5)

# --------------------------------------------- #
# Lets take a look at what is in the dataframe
# --------------------------------------------- #

# Number of unique values of entity_code
length(unique(df.entity_code))
println("Number of unique entity_code: ", length(unique(df.entity_code)))

# get year (first 4 digits of observed_at)
df.year = parse.(Int, first.(string.(df.observed_at), 4))

# Count of rows by year
year_counts = combine(groupby(df, :year), nrow => :count)

# sort df by observed_at
sort!(df, :observed_at, rev=true)

# Get all rows for a specific entity_code
filtered_rows = df[df.entity_code .== "MK25", :]