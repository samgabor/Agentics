# crew_latest_fec_filings_summaries.py
# -----------------------------------------------------------
# CrewAI example that:
#   1) connects to your FEC MCP (fec_info_mcp.py) and a Search MCP via stdio,
#   2) asks the agent to fetch the latest 5 e-file filings and write 1 paragraph per committee,
#   3) sets `generated_at` to the most recent filing timestamp (receipt_date/filed_date/load_timestamp).
#
# Env you must set before running:
#   - FEC_API_KEY (or OPENFEC_API_KEY) for the FEC MCP
#   - MCP_FEC_SERVER_PATH      -> path to fec_info_mcp.py
#   - MCP_SEARCH_SERVER_PATH   -> path to your search MCP server
#
# Run:
#   python .\crew_latest_fec_filings_summaries.py
#
# -----------------------------------------------------------



# ---- Pydantic Field monkey-patch (place at VERY TOP) ----
# gets rid of annoying warnings
import pydantic as _p
import pydantic.fields as _pf

_OrigField = _pf.Field

def _FieldPatched(*args, **kwargs):
    # Keys commonly (and incorrectly) passed directly to Field(...)
    jsonish = {
        "items", "anyOf", "allOf", "oneOf", "enum", "properties",
        "format", "examples", "pattern",
        "minItems", "maxItems", "minimum", "maximum",
        "exclusiveMinimum", "exclusiveMaximum", "const", "nullable",
        # people sometimes also pass "type" etc.
        "type"
    }

    # Merge moved keys into json_schema_extra
    jse = kwargs.get("json_schema_extra")
    if jse is None:
        jse = {}
    elif not isinstance(jse, dict):
        jse = {"_orig": jse}

    moved = False
    for k in list(kwargs.keys()):
        if k in jsonish:
            jse[k] = kwargs.pop(k)
            moved = True
    if moved:
        kwargs["json_schema_extra"] = jse

    return _OrigField(*args, **kwargs)

# Patch both the internal and the public alias
_pf.Field = _FieldPatched
_p.Field = _FieldPatched
#-----------------------------------------------------------


# another patch to silence the "There is no current event loop" warning
import asyncio
try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())
#-----------------------------------------------------------


# --- std imports ---
import os
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple

# --- crew & mcp imports ---
import yaml
from pydantic import BaseModel, Field
from crewai import Agent, Crew, Task
from crewai_tools import MCPServerAdapter
from mcp import StdioServerParameters

# For one-off programmatic MCP tool call (to compute newest filing timestamp)
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters as StdioParamsForClient
from mcp.types import CallToolRequest

from agentics.core.llm_connections import get_llm_provider


# -----------------------------
# Helpers
# -----------------------------

def _require_env_path(var: str) -> str:
    p = os.getenv(var)
    if not p:
        raise RuntimeError(
            f"Missing environment variable {var}. "
            f'Set it to the MCP server script path, e.g.\n  $env:{var} = "D:\\path\\to\\server.py"'
        )
    if not Path(p).exists():
        raise RuntimeError(f"{var} points to a non-existent path: {p}")
    return p


def _parse_iso_to_utc(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        # date-only "YYYY-MM-DD"
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        # ISO datetime (normalize Z to +00:00)
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


async def _get_newest_filing_ts(fec_server_path: str) -> Optional[str]:
    """
    Programmatically call the FEC MCP tool `latest_filings` to fetch 10 rows,
    then compute the newest timestamp across receipt_date/filed_date/load_timestamp.
    Returns an ISO UTC string (..Z) or None.
    """
    params = StdioParamsForClient(
        command=PY_CMD,
        args=[fec_server_path],
        env={"UV_PYTHON": "3.12", **os.environ},
    )
    async with ClientSession(params) as session:
        # Ensure the tool exists
        tools = (await session.list_tools()).tools
        names = {t.name for t in tools}
        if "latest_filings" not in names:
            return None

        req = CallToolRequest(
            name="latest_filings",
            arguments={
                "committee": None,
                "form_type": None,
                "since": None,
                "until": None,
                "per_page": 5,
                "pages": 1,
                "with_totals": False,
                "show_urls": True,
            },
        )
        res = await session.call_tool(req)

        # FastMCP returns outputs list; grab first json payload
        payload: Optional[Dict[str, Any]] = None
        for out in res.outputs:
            if getattr(out, "type", "") == "json" and getattr(out, "json", None) is not None:
                payload = out.json
                break
        if not payload:
            return None

        filings = payload.get("filings") or []
        candidates: List[datetime] = []
        for f in filings:
            for key in ("receipt_date", "filed_date", "load_timestamp"):
                dt = _parse_iso_to_utc(f.get(key))
                if dt:
                    candidates.append(dt)
        if not candidates:
            return None
        newest = max(candidates)
        return newest.isoformat(timespec="seconds").replace("+00:00", "Z")


# -----------------------------
# Pydantic output models
# -----------------------------

class Reference(BaseModel):
    url: Optional[str] = None
    #Citationauthors: Optional[List[str]] = None
    title: Optional[str] = None
    #relevant_text: Optional[str] = None


class CommitteeSummary(BaseModel):
    committee_id: Optional[str] = None
    committee_name: Optional[str] = None
    paragraph: Optional[str] = Field(
        None,
        description="One concise, neutral paragraph (3–6 sentences) summarizing public info about the committee."
    )
    refernces: List[Reference] = Field(default_factory=list, description="Citations used for the paragraph")


# 1) Add this field to LatestFilingsSummaries
class LatestFilingsSummaries(BaseModel):
    title: str = Field(
        default="Summaries for committees in the last 5 OpenFEC e-file filings",
        description="Document title"
    )
    # NEW: carrier for the newest filing timestamp (as returned by the agent)
    source_latest_timestamp: Optional[str] = Field(
        default=None,
        description="Most recent filing timestamp (receipt_date/filed_date/load_timestamp) from latest_filings."
    )
    generated_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        description="UTC timestamp of when this report was generated (will be overwritten from source_latest_timestamp if present)"
    )
    items: List[CommitteeSummary] = Field(
        default_factory=list,
        description="One entry per unique committee found in latest filings"
    )


# -----------------------------
# Required environment parameters
# -----------------------------
python_path = _require_env_path("MCP_PYTHON_PATH")
fec_server_path = _require_env_path("MCP_FEC_SERVER_PATH")
search_server_path = _require_env_path("MCP_SEARCH_SERVER_PATH")

# -----------------------------
# MCP servers (stdio)
# -----------------------------
# FEC MCP server (FastMCP server exposing `latest_filings`)
#   Set MCP_PYTHON_PATH to the path of the python executable from the environment
#   Set MCP_FEC_SERVER_PATH to the path of fec_info_mcp.py
fec_params = StdioServerParameters(
    command=python_path,  # use "python" on Windows if needed
    args=[os.getenv("MCP_FEC_SERVER_PATH")],
    env={"UV_PYTHON": "3.12", **os.environ},
)

#   Set MCP_PYTHON_PATH to the path of the python executable from the environment
#   Set MCP_SEARCH_SERVER_PATH to the path of fec_info_mcp.py
search_params = StdioServerParameters(
    command=python_path,
    args=[os.getenv("MCP_SEARCH_SERVER_PATH")],
    env={"UV_PYTHON": "3.12", **os.environ},
)


# -----------------------------
# Crew (Agent + Task)
# -----------------------------

with (
    MCPServerAdapter(fec_params) as fec_tools,
    MCPServerAdapter(search_params) as search_tools,
    # MCPServerAdapter(fetch_params) as fetch_tools,
):
    print(f"Available FEC tools:    {[t.name for t in fec_tools]}")
    print(f"Available Search tools: {[t.name for t in search_tools]}")
    tools = fec_tools + search_tools  # + fetch_tools

    doc_agent = Agent(
        role="FEC Filing Summarizer",
        goal=(
            "Use the available MCP tools to fetch the most recent 5 OpenFEC e-file filings, "
            "identify unique committees, then perform light web searches and produce ONE short, "
            "neutral paragraph per committee summarizing publicly available information."
        ),
        backstory=(
            "You consult OpenFEC for latest filings and public sources for context. "
            "You avoid speculation and keep a neutral tone."
        ),
        tools=tools,
        reasoning=False,
        reasoning_steps=10,
        memory=False,
        verbose=True,
        llm=get_llm_provider("gemini")
        # If you have a helper like get_llm_provider("openai"), pass llm=... here
    )

    doc_task = Task(
        description="""\
    Step 1) Call the FEC MCP tool `latest_filings` with:
    {"committee": null, "form_type": null, "since": null, "until": null, "per_page": 5, "pages": 1,
    "with_totals": false, "show_urls": true}
    From the returned rows, compute the most recent timestamp across fields (in order of preference):
    receipt_date, filed_date, load_timestamp.
    Set this as `source_latest_timestamp` (ISO8601 UTC, e.g. 2025-09-21T13:40:55Z).
    Then deduplicate the filings by `committee_id` (preserve first occurrence order).

    Step 2) For each unique committee, call the Search MCP tool (named "search") with:
    {"query": "<committee_name> <committee_id>", "limit": 5}
    Prefer reputable sources (official sites, FEC pages, major outlets). Capture titles/URLs.

    Step 3) For each committee, write ONE short paragraph (3–6 sentences), neutral tone, factual only,
    based strictly on the search results. Include simple [#] markers in-text and map them to the sources you used.
    If sources are too sparse, say so for that committee.

    Output a LatestFilingsSummaries object with:
    - source_latest_timestamp: (from Step 1)
    - items[]: CommitteeSummary entries (committee_id, committee_name, paragraph, citations).
    """,
        expected_output=(
            "A LatestFilingsSummaries pydantic object with source_latest_timestamp and items[]."
        ),
        agent=doc_agent,
        output_pydantic=LatestFilingsSummaries,
    )


    crew = Crew(
        agents=[doc_agent],
        tasks=[doc_task],
        verbose=True,
    )

    # --- Run the crew ---
    result = crew.kickoff()

    if getattr(result, "pydantic", None):
        # Overwrite generated_at from source_latest_timestamp if the agent provided it
        st = result.pydantic.source_latest_timestamp
        if st:
            # Normalize *Z to +00:00 and format back to Z
            try:
                ts = datetime.fromisoformat(st.replace("Z", "+00:00")).astimezone(timezone.utc)
                result.pydantic.generated_at = ts.isoformat(timespec="seconds").replace("+00:00", "Z")
            except Exception:
                # fall back to now if the agent gave an unexpected format
                result.pydantic.generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        else:
            # fall back to now
            result.pydantic.generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

        # Print pretty YAML with Unicode
        print(yaml.dump(result.pydantic.model_dump(), sort_keys=False, allow_unicode=True, width=1000))
    else:
        print(result)

