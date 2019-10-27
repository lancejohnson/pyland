# Pyland

This is built to aggregate raw land listings.  We extract the data and transform it into a usable function.

# TODOs

- Logging, to see when failures occur
- Add parameter to county urls to sort by newest

# Structure

1) Scrape every land serp for each county
    - Generate the paginated URLs
    - Scrape the paginated URLs concurrently
2) Transform data into usable array
3) Store in DB

# Questions
1) How should I structure my files with db calls?  Should it be in a separate file?  E.g. one file that handles interactions with the DB and one that messes with the data once I've got it?