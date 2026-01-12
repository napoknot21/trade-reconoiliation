from __future__ import annotations

import os
import polars as pl

from dotenv import load_dotenv

load_dotenv()

# Application settings and values
APPLICATION_ID=os.getenv("APPLICATION_ID")
SECRET_VALUE_ID=os.getenv("SECRET_VALUE_ID")

OBJECT_ID=os.getenv("OBJECT_ID")
TENANT_ID=os.getenv("TENANT_ID")
SECRET_ID=os.getenv("SECRET_ID")


# Permissions + Scopes
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SCOPES = ["https://graph.microsoft.com/.default"]
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"


