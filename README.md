# Texas UIL Concert and Sight-Reading History Dashboard

#### Video Demo:  [<URL HERE>](https://www.youtube.com/watch?v=dyj9p79irfA)
#### Description:

This is a maintained database going back to 2005 based on the Texas UIL records found on [Texas Music Forms](www.texasmusicforms.com).
The "C&SR Results" tab gives you filtering tools to view results based on school, song name, year, etc. The "PML" tab attempts to match the song entries inputted by the director to a song on the official pml list. That gives certain metrics like number of times performed, average score, and associated sight reading score. 

#### Song Score

Song Score is a metric derived from a few factors to give each pml song a rating based on UIL results. It factors in how often the song is performed (percentile of performers in grade since debut) and concert rating over expected. This gives a higher rating to songs that get a "1" when other groups at the contest were less likely to get "1's" that way it normalizes results as sthe average score changes over time. 

#### Libraries

This Dashboard was created with help from Streamlit, Fuzzywuzzy, Pandas, and Selenium.
