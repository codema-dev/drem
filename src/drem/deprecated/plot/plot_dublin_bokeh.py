from bokeh.models.widgets import Dropdown
from bokeh.io import output_file, show
from bokeh.layouts import widgetbox, row, column
from bokeh.models import Slider, HoverTool
from bokeh.io import curdoc, output_notebook
from bokeh.palettes import brewer
from bokeh.models import GeoJSONDataSource, LinearColorMapper, ColorBar
from bokeh.plotting import figure
from bokeh.io import output_notebook, show, output_file
import os
import json
import sys
from pathlib import Path
import geopandas as gpd
import pandas as pd

from codema_drem.utilities.paths import BASE_DIR, DATA_DIR, PLOT_DIR

census2016_with_ber_avgs = gpd.read_file(DATA_DIR / "interim" / "archetype_model")

# -----------------------
# Sources:
# Bokeh map interactivity source:
# https://towardsdatascience.com/a-complete-guide-to-an-interactive-geographical-map-using-python-f4c5197e23e0
# Bokeh dropdown menu source:
# https://docs.bokeh.org/en/latest/docs/user_guide/interaction/widgets.html
# -----------------------

# Read data to json.
merged_json = json.loads(census2016_with_ber_avgs.to_json())

# Convert to String like object.
json_data = json.dumps(merged_json)

# Input GeoJSON source that contains features for plotting.
geosource = GeoJSONDataSource(geojson=json_data)

# Define a sequential multi-hue color palette.
palette = brewer["YlGnBu"][8]

# Reverse color order so that dark blue is highest obesity.
palette = palette[::-1]

# Instantiate LinearColorMapper that linearly maps numbers in a range,
# into a sequence of colors. Input nan_color.
min_value = census2016_with_ber_avgs["DeliveredEnergyMainSpace"].min()
max_value = census2016_with_ber_avgs["DeliveredEnergyMainSpace"].max()
color_mapper = LinearColorMapper(
    palette=palette, low=min_value, high=max_value, nan_color="#d9d9d9"
)

# Add hover tool
hover = HoverTool(
    tooltips=[
        ("Electoral District", "@EDs"),
        ("Space Heat [kWh/year]", "@DeliveredEnergyMainSpace"),
    ]
)

# Create color bar.
color_bar = ColorBar(
    color_mapper=color_mapper,
    label_standoff=8,
    width=500,
    height=20,
    border_line_color=None,
    location=(0, 0),
    orientation="horizontal",
)

# Create figure object.
p = figure(
    title="Dublin Residential Demand @Electoral-District-level 2016",
    plot_height=950,
    plot_width=950,
    toolbar_location=None,
    tools=[hover],
)
p.xgrid.grid_line_color = None
p.ygrid.grid_line_color = None

# Add patch renderer to figure.
p.patches(
    "xs",
    "ys",
    source=geosource,
    fill_color={"field": "DeliveredEnergyMainSpace", "transform": color_mapper},
    line_color="black",
    line_width=0.25,
    fill_alpha=1,
)

# Specify figure layout.
p.add_layout(color_bar, "below")

# Must define dropdown before function to call
# Source: https://stackoverflow.com/questions/39100095/how-to-capture-value-of-dropdown-widget-in-bokeh-python
menu = [
    ("Space Heat Demand [MWh/year]", "DeliveredEnergyMainSpace"),
    ("Hot Water Demand [MWh/year]", "DeliveredEnergyMainWater"),
    ("Total Number of Households", "Total_HH"),
]
dropdown = Dropdown(
    label="Click here to change the data being mapped",
    button_type="warning",
    menu=menu,
    orientation="vertical",
)


def return_full_column_name(select):

    if select == "DeliveredEnergyMainSpace":
        return "Space Heat Demand [MWh/year]"

    if select == "DeliveredEnergyMainWater":
        return "Hot Water Demand [MWh/year]"

    if select == "Total_HH":
        return "Total Number of Households"


# Define the callback function: update_plot
def update_plot(attr, old, new):

    select = dropdown.value

    min_value = census2016_with_ber_avgs[select].min()
    max_value = census2016_with_ber_avgs[select].max()
    color_mapper = LinearColorMapper(
        palette=palette, low=min_value, high=max_value, nan_color="#d9d9d9"
    )

    # Add hover tool
    hover = HoverTool(
        tooltips=[
            ("Small Area", "@SA"),
            (return_full_column_name(select), "@" + select),
        ]
    )
    p.tools = [hover]
    # Add patch renderer to figure.
    p.patches(
        "xs",
        "ys",
        source=geosource,
        fill_color={"field": select, "transform": color_mapper},
        line_color="black",
        line_width=0.25,
        fill_alpha=1,
    )
    return None


dropdown.on_change("value", update_plot)

# Make a column layout of widgetbox(slider) and plot, and add it to the current document
layout = row(p, widgetbox(dropdown))
curdoc().add_root(layout)

# Display plot inline in Jupyter notebook
# output_notebook()
# output_file("Dublin_SAs.html")

# Display plot
show(layout)
