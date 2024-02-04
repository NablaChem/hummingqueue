import streamlit as st
import altair as alt
import pandas as pd
import human_readable
import requests
import datetime as dt
from streamlit_autorefresh import st_autorefresh


primary = "#1F3664"
secondary = "#dd9633"

st.set_page_config(
    page_title="Hummingqueue Dashboard",
    layout="wide",
)
hide_streamlit_style = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
.block-container {padding-top: 0}
</style>

"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)
# refresh every minute
st_autorefresh(interval=60 * 1000)

st.title("Hummingqueue Dashboard")
st.text("The data is updated once a minute.")
queue, datacenters = st.tabs(["Queue", "Datacenters"])


result = requests.get("http://hmq.nablachem.org/queue/inspect")

rows = []
for minutes_ago, data in result.json().items():
    data["age"] = -int(minutes_ago)
    rows.append(data)
df = pd.DataFrame(rows)
df = df.sort_values("age", ascending=False)


available = (
    alt.Chart(df)
    .mark_circle(opacity=0.6, color=primary)
    .mark_line(opacity=0.6, color=primary)
    .encode(x="age", y="cores_used")
    .properties(height=200)
)
used = (
    alt.Chart(df)
    .mark_area(opacity=0.6, color=primary)
    .encode(x="age", y="cores_available")
)

au = alt.layer(available, used)
au.layer[0].encoding.y.title = "Available / used cores"
au.layer[0].encoding.x.title = "Time ago [minutes]"


with queue:
    st.altair_chart(
        au,
        use_container_width=True,
    )

tasks = (
    alt.Chart(df)
    .mark_line(color=primary)
    .encode(x="age", y=alt.Y("tasks_running").axis(titleColor=primary))
    .properties(height=200)
)
tasks2 = (
    alt.Chart(df)
    .mark_line(color=secondary)
    .encode(x="age", y=alt.Y("tasks_queued").axis(titleColor=secondary))
)

c = alt.layer(tasks, tasks2).resolve_scale(y="independent")
c.layer[0].encoding.y.title = "Running tasks"
c.layer[1].encoding.y.title = "Queued tasks"
c.layer[0].encoding.x.title = "Time ago [minutes]"
with queue:
    st.altair_chart(
        c,
        use_container_width=True,
    )

# tags
result = requests.get("http://hmq.nablachem.org/tags/inspect")
df = pd.DataFrame(result.json())
df = df.sort_values("updated", ascending=False)
del df["ts"]
import time

df.received = time.time() - df.received
df.updated = time.time() - df.updated
df["total"] = df.completed + df.failed + df.deleted + df.pending + df.queued
df["progress"] = ((df.completed + df.failed + df.deleted) / df.total) * 100
df.progress.fillna(100, inplace=True)


def T(x):
    return human_readable.time_delta(dt.timedelta(seconds=x)) + " ago"


def T2(x):
    minutes = int(x // 60)
    seconds = int(x % 60)
    hours = int(minutes // 60)
    minutes = minutes % 60
    days = int(hours // 24)
    hours = hours % 24
    years = int(days // 365)
    days = days % 365
    if years > 0:
        if days == 0:
            return f"{years}y"
        return f"{years}y {days}d"
    if days > 0:
        if hours == 0:
            return f"{days}d"
        return f"{days}d {hours}h"
    if hours > 0:
        if minutes == 0:
            return f"{hours}h"
        return f"{hours}h {minutes}m"
    if minutes > 0:
        if seconds == 0:
            return f"{minutes}m"
        return f"{minutes}m {seconds}s"
    if seconds == 0:
        return "<1s"
    return f"{seconds}s"


df.received = df.received.apply(T)
df.updated = df.updated.apply(T)
df.computetime = df.computetime.apply(T2)
st.dataframe(
    df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "progress": st.column_config.ProgressColumn(
            "Progress",
            help="Tag completion",
            format="%d%%",
            min_value=0,
            max_value=100,
        ),
    },
    column_order="tag total progress completed failed queued pending deleted computetime received updated".split(),
)


# datacenters
result = requests.get("http://hmq.nablachem.org/datacenters/inspect")
rows = []
for datacenter, last_seen in result.json().items():
    explained = human_readable.time_delta(dt.timedelta(seconds=last_seen))
    rows.append({"datacenter": datacenter, "last seen": explained + " ago"})
df = pd.DataFrame(rows)
with datacenters:
    st.dataframe(df)
