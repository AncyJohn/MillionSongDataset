setwd('M:/outputs/data')
library(dplyr) 
library(ggplot2)
library(rgdal)
library(broom)
library(sp)
library(plotly)
library(raster)
library(sf)

# Input data on precipitation
NZ_plot = read.csv('PRCP_map.csv')
str(NZ_plot)
df = filter(NZ_plot, CODE != 'CODE')
df$CODE = as.character(df$CODE)
df$YEAR = as.integer(df$YEAR)
df$AVERAGE_PRCP = as.numeric(df$AVERAGE_PRCP)
df$NAME = as.character(df$NAME)
str(df)
df <- df[3:4]
df %>% 
  ggplot( aes(x=AVERAGE_PRCP)) + 
  geom_histogram(bins=20, fill='skyblue', color='#69b3a2') + scale_x_log10()

# Geospatial input file

shp <- st_read("ne_10m_admin_0_countries_lakes/ne_10m_admin_0_countries_lakes.shp")
head(shp)

shp1 <- shp[data['ADMIN'], polygons[1:255] , ]

 
shp_fortified <- tidy(shp1, region = 'ADMIN')

plt<- ggplot() +
  geom_polygon(data = shp_fortified, aes( x = long, y = lat, group = group), fill="white", color="grey") +
  theme_void() +
  coord_map()
ggplotly(plt)

# Merge data
shp_merged <- merge(shp_fortified, df, by.shp_fortified = "ADMIN", by.df = "NAME")
plt<- ggplot() +
  geom_polygon(data = shp_merged, aes(fill = nb_equip, x = long, y = lat, group = group)) +
  theme_void() +
  coord_map()
ggplotly(plt)


# Reference: https://stackoverflow.com/questions/30561889/readogr-cannot-open-file
# Reference: https://stackoverflow.com/questions/30561889/readogr-cannot-open-file