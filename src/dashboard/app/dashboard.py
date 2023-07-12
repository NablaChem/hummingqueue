import streamlit as st
import pandas as pd
import requests

st.set_page_config(page_title="Hummingqueue Dashboard", layout="wide")

st.title("Hummingqueue Dashboard")
col1, col2 = st.columns(2)
col1.caption("Queue depth")

result = requests.get("http://hmq/usage/inspect")

rows = []
for minutes_ago, data in result.json().items():
    data["age"] = int(minutes_ago)
    rows.append(data)
df = pd.DataFrame(rows)
df = df.sort_values("age")


col1.line_chart(df, x="age")
# col2.caption("Tags")
# for tag in "foo bar sdjfhwsökgdjhsökgj".split():
#    col2.progress(0.54, tag)#

# col1, col2 = st.columns(2)
# col1.caption("Hello")
