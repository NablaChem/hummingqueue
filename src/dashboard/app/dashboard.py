import streamlit as st
import altair as alt
import pandas as pd
import human_readable
import requests
import datetime as dt
import time
from streamlit_autorefresh import st_autorefresh

URL = "http://hmq"

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


result = requests.get(f"{URL}/queue/inspect")
print(result.json())

rows = []
for minutes_ago, data in result.json().items():
    data["age"] = -int(minutes_ago)
    rows.append(data)
df = pd.DataFrame(rows)
df = df.sort_values("age", ascending=False)

try:
    total_coretime = next(df.query("age == 0").iterrows())[1]["total_coretime"]
    total_jobs = next(df.query("age == 0").iterrows())[1]["total_jobs"]
except:
    total_coretime = 0
    total_jobs = 0
used = (
    alt.Chart(df)
    .mark_circle(opacity=0.6, color=primary)
    .mark_line(opacity=0.6, color=primary)
    .encode(x="age", y="cores_used")
    .properties(height=200)
)
domain = max(max(10, df.cores_allocated.max()), df.cores_used.max())
allocated = (
    alt.Chart(df)
    .mark_area(opacity=0.6, color=primary)
    .encode(
        x="age",
        y=alt.Y("cores_allocated", scale=alt.Scale(domain=(0, domain))).axis(
            titleColor=primary,
            title="Allocated / used cores",
        ),
    )
)

available = (
    alt.Chart(df)
    .mark_line(color=secondary)
    .encode(
        x="age",
        y=alt.Y("cores_available").axis(titleColor=secondary, title="Available cores"),
    )
)
c = alt.layer(allocated + used, available).resolve_scale(y="independent")

with queue:
    main, metric = st.columns([0.9, 0.1])
    with main:
        st.altair_chart(
            c,
            use_container_width=True,
        )
    with metric:
        st.metric(
            "Total CPU hours",
            f"{total_coretime / 3600 / 1e6:.2f} M",
            border=False,
        )

domain = max(10, df.tasks_running.max())
tasks = (
    alt.Chart(df)
    .mark_line(color=primary)
    .encode(
        x="age",
        y=alt.Y("tasks_running", scale=alt.Scale(domain=(0, domain))).axis(
            titleColor=primary
        ),
    )
    .properties(height=200)
)
domain = max(10, df.tasks_queued.max())
tasks2 = (
    alt.Chart(df)
    .mark_line(color=secondary)
    .encode(
        x="age",
        y=alt.Y("tasks_queued", scale=alt.Scale(domain=(0, domain))).axis(
            titleColor=secondary
        ),
    )
)

c = alt.layer(tasks, tasks2).resolve_scale(y="independent")
c.layer[0].encoding.y.title = "Running tasks"
c.layer[1].encoding.y.title = "Queued tasks"
c.layer[0].encoding.x.title = "Time ago [minutes]"

with queue:
    main, metric = st.columns([0.9, 0.1])
    with main:
        st.altair_chart(
            c,
            use_container_width=True,
        )
    with metric:
        st.metric(
            "Total tasks",
            f"{total_jobs  / 1e6:.2f} M",
            border=False,
        )
# tags
result = requests.get(f"{URL}/tags/inspect")
df = pd.DataFrame(result.json())
if len(df) == 0:
    df = None
    with queue:
        st.write("No tag history available.")
else:
    df = df.sort_values("updated", ascending=False)
    del df["ts"]

    df.received = time.time() - df.received
    df.updated = time.time() - df.updated
    df["total"] = df.completed + df.failed + df.deleted + df.pending + df.queued
    df["progress"] = ((df.completed + df.failed + df.deleted) / df.total) * 100
    df.fillna({"progress": 0}, inplace=True)
    df.received = df.received.apply(T)
    df.updated = df.updated.apply(T)
    df.computetime = df.computetime.apply(T2)
    with queue:
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
result = requests.get(f"{URL}/datacenters/inspect")
rows = []
for datacenter, last_seen in result.json().items():
    explained = human_readable.time_delta(dt.timedelta(seconds=last_seen))
    rows.append({"datacenter": datacenter, "last seen": explained + " ago"})
df = pd.DataFrame(rows)
with datacenters:
    st.dataframe(df)
