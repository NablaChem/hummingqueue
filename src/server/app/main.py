from multiprocessing.sharedctypes import Value
from pydantic import BaseModel, Field, constr, root_validator
from typing import List, Optional
from fastapi.responses import HTMLResponse
import threading
import inspect
from . import helpers
from . import auth
from . import maintenance

counter = 0

from fastapi import FastAPI, Request, Response

app = FastAPI(
    docs_url=None,
    redoc_url=None,
    title="Hummingqueue API",
    description="""## Motivation
    
Hummingqueue is an open-source, self-hosted, distributed, and scalable job queue for scientific computing.
""",
)

# add routes
routes = ["compute", "communication", "security"]
for route in routes:
    module = __import__(f"app.routers.{route}", fromlist=["app"])

    # for objname, obj in inspect.getmembers(module, inspect.isclass):
    #    if issubclass(obj, BaseModel):
    #        helpers.build_schema_example(obj)

    app.include_router(module.app)


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


maintenance_thread = threading.Thread(target=maintenance.flow_control)
maintenance_thread.start()
