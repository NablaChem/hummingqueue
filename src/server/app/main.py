from fastapi import FastAPI
import os
import sentry_sdk
import asyncio
from fastapi.responses import HTMLResponse
import threading
import random
import datetime as dt
from . import maintenance
from contextlib import asynccontextmanager
from .routers import security
from starlette_compress import CompressMiddleware

counter = 0


def endpoint_importance_sampling(event, hint):
    sample_rate = 0.001
    report_everything_above = dt.timedelta(milliseconds=500)
    report_very_rarely_below = dt.timedelta(milliseconds=5)
    very_rarely_factor = 0.001

    start_time = event.get("start_timestamp")
    end_time = event.get("timestamp")
    if start_time and end_time:
        try:
            start_time = dt.datetime.fromisoformat(start_time)
            end_time = dt.datetime.fromisoformat(end_time)
            duration = end_time - start_time

            if duration >= report_everything_above:
                sample_rate = 1
            if duration <= report_very_rarely_below:
                sample_rate *= very_rarely_factor

        except Exception:
            pass

    if random.random() < sample_rate:
        return event


sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    before_send_transaction=endpoint_importance_sampling,
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(maintenance.flow_control())
    yield
    # optional cleanup code here

app = FastAPI(
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    title="Hummingqueue API",
    description="""## Motivation
    
Hummingqueue is an open-source, self-hosted, distributed, and scalable job queue for scientific computing.
""",
)

# enable transparent compression
app.add_middleware(CompressMiddleware)

# add routes
routes = [
    "compute",
    "communication",
    "security",
    "function",
    "queue",
    "results",
    "tags",
    "tasks",
]
for route in routes:
    module = __import__(f"app.routers.{route}", fromlist=["app"])

    app.include_router(module.app)
app.version = str(security.VERSION)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def rapidoc():
    return (
        """
        <!doctype html>
        <html>
            <head>
                <meta charset="utf-8">
                <script 
                    type="module" 
                    src="https://unpkg.com/rapidoc/dist/rapidoc-min.js"
                ></script>
            </head>
            <body>
            <style>
                    rapi-doc::part(section-operation-tag) {
                    display:none;
                    }
            </style>
            <rapi-doc 
                spec-url='"""
        + app.openapi_url
        + """'
                render-style="focused" 
                show-header="false" 
                allow-server-selection="false" 
                allow-authentication="false" 
                schema-description-expanded="true" 
                default-schema-tab="schema"
                primary-color="#1F3664"
                show-method-in-nav-bar="as-colored-block"
                nav-bg-color="#1F3664"
                nav-text-color="#ffffff"
                nav-accent-color="#dd9633"
                font-size="largest"
                schema-description-expanded="true"
                schema-style="table"
                use-path-in-nav-bar="true"
                sort-endpoints-by="path"
                ></rapi-doc>
            </body> 
        </html>
    """
    )
