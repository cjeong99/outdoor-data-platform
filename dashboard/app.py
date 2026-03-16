import streamlit as st
import duckdb
import pandas as pd
import pydeck as pdk

st.set_page_config(layout="wide")

st.title("🏕 Camping Explorer")

# DuckDB connection
con = duckdb.connect()

# Load campsite data
df = con.execute("""
SELECT
    c.campsite_id,
    c.campsite_name,
    f.facility_name,
    r.recarea_name,
    c.latitude,
    c.longitude
FROM read_parquet('data/silver/campsites.parquet') c
JOIN read_parquet('data/silver/facilities.parquet') f
ON c.facility_id = f.facility_id
JOIN read_parquet('data/silver/recareas.parquet') r
ON f.recarea_id = r.recarea_id
JOIN read_parquet('data/silver/campsite_equipment.parquet') e
ON c.campsite_id = e.campsite_id
WHERE e.equipment_name = 'RV'
AND c.latitude IS NOT NULL
""").df()

# Sidebar filters
st.sidebar.header("Filters")

parks = sorted(df["recarea_name"].dropna().unique())

selected_park = st.sidebar.selectbox(
    "Select Park",
    ["All"] + parks
)

park_search = st.sidebar.text_input("Search Park")

# Apply filters
filtered_df = df.copy()

if selected_park != "All":
    filtered_df = filtered_df[
        filtered_df["recarea_name"] == selected_park
    ]

if park_search:
    filtered_df = filtered_df[
        filtered_df["recarea_name"].str.contains(park_search, case=False)
    ]

# Rename for map
filtered_df = filtered_df.rename(columns={
    "latitude": "lat",
    "longitude": "lon"
})

# Map layer
layer = pdk.Layer(
    "ScatterplotLayer",
    data=filtered_df,
    get_position='[lon, lat]',
    get_radius=1000,
    get_fill_color=[255, 0, 0],
    pickable=True
)

# Map view
view_state = pdk.ViewState(
    latitude=37,
    longitude=-96,
    zoom=4
)

# Map
st.pydeck_chart(
    pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "html": """
            <b>Park:</b> {recarea_name}<br/>
            <b>Campground:</b> {facility_name}<br/>
            <b>Campsite:</b> {campsite_name}
            """
        }
    )
)

# Table display
st.subheader("Filtered Campsites")

st.dataframe(
    filtered_df[
        [
            "recarea_name",
            "facility_name",
            "campsite_name"
        ]
    ]
)



st.title("🏕 Camping Overview")

con = duckdb.connect()

# RV campsite query
rv_sites = con.execute("""
SELECT
    c.campsite_id,
    c.campsite_name,
    f.facility_name,
    r.recarea_name,
    c.latitude,
    c.longitude
FROM read_parquet('data/silver/campsites.parquet') c
JOIN read_parquet('data/silver/facilities.parquet') f
ON c.facility_id = f.facility_id
JOIN read_parquet('data/silver/recareas.parquet') r
ON f.recarea_id = r.recarea_id
JOIN read_parquet('data/silver/campsite_equipment.parquet') e
ON c.campsite_id = e.campsite_id
WHERE e.equipment_name = 'RV'
AND c.latitude IS NOT NULL
""").df()


rv_sites = rv_sites.rename(columns={
    "latitude": "lat",
    "longitude": "lon"
})

layer = pdk.Layer(
    "ScatterplotLayer",
    data=rv_sites,
    get_position='[lon, lat]',
    get_radius=500,
    get_fill_color=[255, 0, 0],
    pickable=True
)

view_state = pdk.ViewState(
    latitude=37.5,
    longitude=-96,
    zoom=4
)

st.pydeck_chart(pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    map_style="road",
    tooltip={
    "html": """
    <b>Park:</b> {recarea_name} <br/>
    <b>Campground:</b> {facility_name} <br/>
    <b>Campsite:</b> {campsite_name}
    """
    }
))





st.title("🏕 Camping Explorer Map")

con = duckdb.connect("data/camping.db")

# recarea + location
df = con.execute("""
SELECT
    recarea_name,
    latitude,
    longitude
FROM read_parquet('data/silver/recareas.parquet')
WHERE latitude IS NOT NULL
""").df()

df = df.rename(columns={
    "latitude": "lat",
    "longitude": "lon"
})

st.subheader("Recreation Areas Map")

st.map(df)


st.title("🏕 Camping Data Explorer")

con = duckdb.connect("data/camping.db")

df = con.execute("""
SELECT *
FROM campsites_per_recarea
ORDER BY total_campsites DESC
LIMIT 20
""").df()

st.subheader("Top Parks by Campsites")

st.dataframe(df)

st.subheader("RV Friendly Campsites")

rv = con.execute("""
SELECT *
FROM rv_sites
LIMIT 20
""").df()

st.dataframe(rv)
