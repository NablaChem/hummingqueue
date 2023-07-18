import streamlit as st
import pandas as pd
import requests
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Hummingqueue Dashboard", layout="wide")
# refresh every minute
st_autorefresh(interval=60 * 1000)

st.title("Hummingqueue Dashboard")
col1, col2 = st.columns(2)


result = requests.get("http://hmq/usage/inspect")

rows = []
for minutes_ago, data in result.json().items():
    data["age"] = -int(minutes_ago)
    rows.append(data)
df = pd.DataFrame(rows)
df = df.sort_values("age", ascending=False)
col1.caption("Compute resources")
col1.line_chart(df, x="age", y=["cores_available", "cores_used"])
col2.caption("Queue depth")
col2.line_chart(df, x="age", y=["tasks_queued"])
col2.line_chart(df, x="age", y=["tasks_running"])
# col2.caption("Tags")
# for tag in "foo bar sdjfhwsökgdjhsökgj".split():
#    col2.progress(0.54, tag)#

# col1, col2 = st.columns(2)
# col1.caption("Hello")
