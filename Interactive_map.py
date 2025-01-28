import pandas as pd
import plotly.express as px

# Load data
print('Getting data...')
sanFranciscoData = pd.read_csv("C:/Users/User/Downloads/sanFranciscoData.csv")
newYorkData = pd.read_csv("C:/Users/User/Downloads/newYorkData.csv")

# Print schemas
sanFranciscoData.info()
newYorkData.info()

# Map columns
mappings = {
    "sf": {
        "zipcode": "Zipcode",
        "avg(Response Time)": "avg(Response Time)",
        "stddev_samp(Response Time)": "stddev(Response Time)",
        "avg(Handling Time)": "avg(Handling Time)",
        "stddev_samp(Handling Time)": "stddev(Handling Time)",
        "Incident Count": "Incident Count",
        "Number of Stations": "Number of Stations",
        "avg(longitude)": "Longitude",
        "avg(latitude)": "Latitude"
    },
    "ny": {
        "ZIP_CODE": "Zipcode",
        "avg (Response Time)": "avg(Response Time)",
        "stddev (Response Time)": "stddev(Response Time)",
        "avg (Handling Time)": "avg(Handling Time)",
        "stddev (Handling Time)": "stddev(Handling Time)",
        "Incident Count": "Incident Count",
        "Number of Stations": "Number of Stations",
        "Longitude": "Longitude",
        "Latitude": "Latitude"
    }
}

sanFranciscoData.rename(columns=mappings["sf"], inplace=True)
newYorkData.rename(columns=mappings["ny"], inplace=True)

# Reorder the columns if not done already
order = [
    'Zipcode',
    'avg(Response Time)',
    'stddev(Response Time)',
    'avg(Handling Time)',
    'stddev(Handling Time)',
    'Incident Count',
    'Number of Stations',
    'Longitude',
    'Latitude'
]

sanFranciscoData = sanFranciscoData[order]
newYorkData = newYorkData[order]

# Merge the two dataFrames
mergedData = pd.concat([sanFranciscoData, newYorkData], axis=0, ignore_index=True)

# Set data for the plot
data = mergedData

# Clean the relevant columns by dropping rows with NaN values
data = data.dropna(
    subset=['Longitude', 'Latitude', 'avg(Response Time)', 'Incident Count']
)

# # Alternatively, fill NaN values with defaults (uncomment if preferred):
# data = data.fillna(0)

# Draw figure
fig = px.scatter_mapbox(
    data,
    lon=data['Longitude'],
    lat=data['Latitude'],
    zoom=3,
    color=data['avg(Response Time)'],
    size=data['Incident Count'],
    size_max = 50,
    width=1200,
    height=900,
    title='Fire Incidents per Zipcode',
    hover_name="Zipcode",
    hover_data={
        "Incident Count": True,
        "avg(Response Time)": True,
        "avg(Handling Time)": True,
        "Number of Stations": True,
        "Longitude": False,
        "Latitude": False,
    }
)

fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r": 0, "t": 50, "l": 0, "b": 10})

# Allow mouse zooming
config = {'scrollZoom': True}

# Show plot
fig.show(config=config)
print('Plot complete.')
