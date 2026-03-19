import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Any

import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh


ROOT = Path(__file__).resolve().parents[1]
API_BASE_URL = os.getenv("CACHEFRONT_API_URL", "http://localhost:8000")
DEFAULT_USER_ID = int(os.getenv("CACHEFRONT_DEFAULT_USER_ID", "1"))
LOGGER = logging.getLogger("cachefront.ui")
PIPELINE_STAGES = ["MYSQL", "DEBEZIUM", "KAFKA", "CONSUMER", "REDIS"]
CACHE_MODE_OPTIONS = ["ttl", "cdc"]
STAGE_LABELS = {
    "MYSQL": "MySQL",
    "DEBEZIUM": "Debezium",
    "KAFKA": "Kafka",
    "CONSUMER": "Consumer",
    "REDIS": "Redis",
}


st.set_page_config(page_title="CacheFront Control Center", page_icon="CF", layout="wide")


def init_session_state() -> None:
    defaults = {
        "selected_user_id": DEFAULT_USER_ID,
        "last_action_feedback": None,
        "docker_warning_ack": False,
        "cache_mode_change_ack": False,
        "cache_mode_feedback": None,
        "is_switching_mode": False,
        "reset_cache_mode_change_ack": False,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


@st.cache_data(ttl=2, show_spinner=False)
def fetch_ui_state(user_id: int) -> dict[str, Any] | None:
    response = requests.get(f"{API_BASE_URL}/ui/state", params={"user_id": user_id, "limit": 12}, timeout=3)
    response.raise_for_status()
    return response.json()


def build_unavailable_response(message: str) -> dict[str, str]:
    return {"message": message}


def fetch_alert_state_from_backend() -> dict[str, Any]:
    try:
        response = requests.get(f"{API_BASE_URL}/observability/alerts", timeout=1.5)
        if response.ok:
            payload = response.json()
            return payload if isinstance(payload, dict) else {}
    except requests.RequestException:
        pass
    except ValueError:
        pass
    return {}


@st.cache_data(ttl=1, show_spinner=False)
def get_alert_state_cached() -> dict[str, Any]:
    return fetch_alert_state_from_backend()


def call_api(method: str, path: str, payload: dict[str, Any] | None = None) -> tuple[bool, Any]:
    try:
        response = requests.request(method, f"{API_BASE_URL}{path}", json=payload, timeout=5)
        if response.headers.get("content-type", "").startswith("application/json"):
            data = response.json()
        else:
            data = build_unavailable_response("The backend returned an unexpected response.")
        if response.ok:
            return True, data
        return False, data if isinstance(data, dict) else build_unavailable_response("The backend request could not be completed.")
    except requests.RequestException:
        return False, build_unavailable_response("Unable to reach backend services right now.")


def load_ui_state(user_id: int) -> tuple[bool, dict[str, Any]]:
    try:
        state = ensure_dict(fetch_ui_state(user_id), context="ui_state")
        return True, state
    except requests.RequestException:
        return False, {}
    except Exception as exc:
        LOGGER.warning("Failed to load UI state: %s", exc)
        return False, {}


def run_docker_command(args: list[str]) -> tuple[bool, str]:
    command = ["docker", "compose", *args]
    try:
        result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
    except OSError as exc:
        return False, str(exc)

    output = (result.stdout or "") + ("\n" + result.stderr if result.stderr else "")
    return result.returncode == 0, output.strip()


def get_compose_status() -> str:
    ok, output = run_docker_command(["ps", "--status", "running", "--services"])
    if not ok:
        return "Docker status unavailable"
    services = [line.strip() for line in output.splitlines() if line.strip()]
    if not services:
        return "System stopped"
    return f"Running: {', '.join(services)}"


def render_badge(text: str, color: str) -> str:
    return f'''
    <div style="
        display:inline-block;
        padding:6px 12px;
        border-radius:20px;
        font-weight:600;
        background-color:{color};
        color:white;
        font-size:14px;
        margin-right:8px;
        margin-bottom:8px;
    ">
        {text}
    </div>
    '''


def status_badge(label: str, color: str) -> str:
    return render_badge(label, color)


def ensure_dict(value: Any, *, context: str) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is not None:
        LOGGER.warning("Expected dict for %s, got %s", context, type(value).__name__)
    return {}


def safe_get(mapping: Any, *keys: str, default: Any = None) -> Any:
    current: Any = mapping
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def mode_badge_color(mode: str) -> str:
    if mode == "cdc":
        return "#2563eb"
    if mode == "ttl":
        return "#f79009"
    return "#6b7280"


def alert_badge_color(severity: str) -> str:
    colors = {
        "high": "#b42318",
        "medium": "#f79009",
        "low": "#facc15",
        "none": "#177245",
    }
    return colors.get(severity, "#344054")


def get_system_status(api_available: bool, is_switching_mode: bool, compose_status: str) -> tuple[str, str]:
    if is_switching_mode:
        return "Starting", "#f79009"
    if api_available:
        return "Running", "#177245"
    if compose_status == "Docker status unavailable" or compose_status == "System stopped":
        return "Stopped", "#b42318"
    return "Starting", "#f79009"


def get_feedback_message(data: Any, fallback: str) -> str:
    if isinstance(data, dict):
        detail = data.get("detail") or data.get("message")
        if isinstance(detail, str) and detail.strip():
            return detail
    return fallback


def on_cache_mode_ack_change() -> None:
    return None


def apply_pending_widget_resets() -> None:
    if st.session_state.pop("reset_cache_mode_change_ack", False):
        st.session_state.cache_mode_change_ack = False


def is_cache_healthy(db_value: Any, cache_value: Any) -> bool:
    db_payload = ensure_dict(db_value, context="db_value")
    cache_payload = ensure_dict(cache_value, context="cache_value")
    if not db_payload or not cache_payload:
        return False

    db_ts = db_payload.get("updated_at")
    cache_ts = cache_payload.get("updated_at")
    if not db_ts or not cache_ts:
        return False

    return db_ts == cache_ts


def render_cache_card(cache_state: dict[str, Any] | None, db_value: dict[str, Any] | None = None) -> None:
    cache_state = ensure_dict(cache_state, context="cache_state")
    if not cache_state:
        st.info("Cache state will appear after the first read or inspection.")
        return

    cache_value = ensure_dict(cache_state.get("value"), context="cache_state.value")
    is_invalidated = cache_state.get("status") != "fresh" or not cache_value

    st.markdown(
        render_badge("Invalidated: Yes" if is_invalidated else "Invalidated: No", "#b42318" if is_invalidated else "#177245"),
        unsafe_allow_html=True,
    )
    st.caption(f"Key: {cache_state.get('key')}")
    ttl_value = cache_state.get("ttl_seconds")
    st.caption(f"TTL seconds: {ttl_value if ttl_value is not None else 'none'}")
    st.json(cache_value or {"message": "No cached value present"})


def render_pipeline(flow: dict[str, Any] | None) -> None:
    flow = ensure_dict(flow, context="last_flow")
    if not flow or not flow.get("flow_id"):
        st.info("No CDC flow captured yet. Insert or update a user to start one.")
        return

    stages = ensure_dict(flow.get("stages"), context="last_flow.stages")
    stage_blocks = []
    for index, stage in enumerate(PIPELINE_STAGES):
        stage_info = ensure_dict(stages.get(stage), context=f"last_flow.stages.{stage}")
        if stage_info:
            color = "#177245"
            border = "#4ad295"
            subtitle = stage_info.get("message", "completed")
        else:
            color = "#1f2937"
            border = "#4b5563"
            subtitle = "waiting"
        stage_blocks.append(
            f"<div style='min-width:120px;padding:1rem;border:2px solid {border};border-radius:18px;'"
            f"background:{color};color:white;text-align:center;box-shadow:0 0 18px rgba(0,0,0,0.15)'>"
            f"<div style='font-size:1rem;font-weight:700'>{STAGE_LABELS[stage]}</div>"
            f"<div style='font-size:0.78rem;opacity:0.9;margin-top:0.35rem'>{subtitle}</div></div>"
        )
        if index < len(PIPELINE_STAGES) - 1:
            arrow_active = stages.get(stage) is not None
            arrow_color = "#4ad295" if arrow_active else "#94a3b8"
            arrow_symbol = "&rarr;"
            stage_blocks.append(
                f"<div style='font-size:1.8rem;color:{arrow_color};padding:0 0.45rem;animation:'"
                f"{'pulse 1.2s infinite' if arrow_active else 'none'}'>{arrow_symbol}</div>"
            )
    html = """
    <style>
    @keyframes pulse {
      0% { opacity: 0.3; transform: translateX(0px); }
      50% { opacity: 1; transform: translateX(4px); }
      100% { opacity: 0.3; transform: translateX(0px); }
    }
    </style>
    <div style='display:flex;align-items:center;gap:0.2rem;overflow-x:auto;padding-bottom:0.5rem'>
    """ + "".join(stage_blocks) + "</div>"
    st.markdown(html, unsafe_allow_html=True)
    st.caption(f"Latest flow: {flow.get('operation')} for user {flow.get('user_id')} | status: {flow.get('status')}")


def explain(text: str, enabled: bool) -> None:
    if enabled:
        st.info(text)


def run_app() -> None:
    init_session_state()
    apply_pending_widget_resets()

    compose_status = get_compose_status()
    api_available, state = load_ui_state(st.session_state.selected_user_id)
    controls_disabled = (not api_available) or st.session_state.is_switching_mode
    system_status_label, system_status_color = get_system_status(
        api_available,
        st.session_state.is_switching_mode,
        compose_status,
    )
    alert_state = get_alert_state_cached()
    db_state = ensure_dict(state.get("db_state"), context="ui_state.db_state") if state else {}
    cache_state = ensure_dict(state.get("cache_state"), context="ui_state.cache_state") if state else {}
    cache_value = ensure_dict(cache_state.get("value"), context="ui_state.cache_state.value") if cache_state else {}
    cache_healthy = is_cache_healthy(db_state, cache_value)

    with st.sidebar:
        st.title("Control Center")
        st.markdown(status_badge(f"System Status: {system_status_label}", system_status_color), unsafe_allow_html=True)
        st.markdown(render_badge("Cache healthy" if cache_healthy else "Cache stale", "#177245" if cache_healthy else "#b42318"), unsafe_allow_html=True)
        st.caption(compose_status)
        explain_mode = st.toggle("Explain Mode", value=True)
        auto_refresh = st.toggle("Auto refresh", value=True)
        refresh_seconds = st.slider("Refresh every (seconds)", min_value=2, max_value=10, value=2)
        selected_user = st.number_input(
            "Observed user id",
            min_value=1,
            value=st.session_state.selected_user_id,
            step=1,
            disabled=controls_disabled,
        )
        st.session_state.selected_user_id = int(selected_user)
        explain(
            "Explain Mode adds plain-language guidance about CDC, cache invalidation, and what each panel means.",
            explain_mode,
        )

    if auto_refresh:
        st_autorefresh(interval=refresh_seconds * 1000, key="ui-autorefresh")

    st.title("CacheFront Local Simulator UI")
    st.caption("Operate the system, observe cache behavior, and watch CDC propagate without opening extra terminals.")

    if st.session_state.is_switching_mode:
        st.info("Switching cache mode... updating system behavior. Please wait a moment.")
    elif not api_available:
        st.warning("System is starting or temporarily unavailable. Please wait while services come online.")

    current_cache_mode = safe_get(state, "cache_mode", default="unknown") if state else "unknown"
    previous_backend_mode = st.session_state.get("last_backend_cache_mode")

    if "selected_cache_mode" not in st.session_state:
        st.session_state.selected_cache_mode = current_cache_mode if current_cache_mode in CACHE_MODE_OPTIONS else CACHE_MODE_OPTIONS[0]

    if current_cache_mode in CACHE_MODE_OPTIONS:
        if previous_backend_mode is None or current_cache_mode != previous_backend_mode:
            st.session_state.selected_cache_mode = current_cache_mode
            st.session_state.last_backend_cache_mode = current_cache_mode
    elif "last_backend_cache_mode" not in st.session_state:
        st.session_state.last_backend_cache_mode = current_cache_mode

    with st.sidebar:
        st.divider()
        st.subheader("Cache Mode")
        st.markdown(status_badge(current_cache_mode.upper(), mode_badge_color(current_cache_mode)), unsafe_allow_html=True)
        st.caption("TTL expires cache entries by time. CDC keeps keys hot and invalidates them from the change stream.")
        st.warning("Switching modes invalidates cached user entries and changes consistency behavior immediately.")
        st.checkbox(
            "I understand this will change system behavior",
            key="cache_mode_change_ack",
            disabled=controls_disabled,
            on_change=on_cache_mode_ack_change,
        )
        selected_mode = st.selectbox(
            "Mode",
            options=CACHE_MODE_OPTIONS,
            key="selected_cache_mode",
            disabled=controls_disabled or not st.session_state.cache_mode_change_ack,
            help="TTL uses time-based expiry. CDC relies on Debezium and Kafka events to invalidate Redis.",
        )
        pending_mode_change = current_cache_mode in CACHE_MODE_OPTIONS and selected_mode != current_cache_mode
        if pending_mode_change:
            st.info(f"Selected mode is {selected_mode}. Click Apply Cache Mode to update the backend.")

        apply_mode_disabled = (
            controls_disabled
            or not st.session_state.cache_mode_change_ack
            or current_cache_mode not in CACHE_MODE_OPTIONS
            or not pending_mode_change
        )
        if st.button("Apply Cache Mode", use_container_width=True, disabled=apply_mode_disabled):
            st.session_state.is_switching_mode = True
            st.session_state.cache_mode_feedback = None
            st.info("Switching cache mode... updating system behavior. Please wait a moment.")
            ok, data = call_api("POST", "/config/cache-mode", {"mode": selected_mode})
            time.sleep(1)
            st.session_state.cache_mode_feedback = (ok, data)
            st.session_state.is_switching_mode = False
            if ok:
                applied_mode = safe_get(data, "cache_mode", default=selected_mode)
                if applied_mode in CACHE_MODE_OPTIONS:
                    st.session_state.last_backend_cache_mode = applied_mode
                fetch_ui_state.clear()
                st.session_state.reset_cache_mode_change_ack = True
            st.rerun()

        if st.session_state.cache_mode_feedback is not None:
            ok, data = st.session_state.cache_mode_feedback
            if ok:
                current_mode = safe_get(data, "cache_mode", default="unknown")
                invalidated = safe_get(data, "invalidated_keys", default=0)
                changed = safe_get(data, "changed", default=False)
                if changed:
                    st.success(f"Cache mode switched to {current_mode}. Invalidated {invalidated} cached key(s).")
                else:
                    st.info(f"Cache mode is already {current_mode}.")
            else:
                st.warning(get_feedback_message(data, "Unable to update cache mode right now. Please try again."))

    left, right = st.columns([1.05, 1.2], gap="large")

    with left:
        st.subheader("Database Control Panel")
        explain(
            "Insert creates a new MySQL row. Update modifies the existing row. Both actions start a CDC flow that the UI tracks across the pipeline.",
            explain_mode,
        )
        with st.form("db-control"):
            user_id = st.number_input("user_id", min_value=1, value=st.session_state.selected_user_id, step=1, disabled=controls_disabled)
            name = st.text_input("name", value="Ada Lovelace", disabled=controls_disabled)
            email = st.text_input("email", value="ada@example.com", disabled=controls_disabled)
            status = st.selectbox("status", ["active", "inactive", "suspended"], index=0, disabled=controls_disabled)
            insert_col, update_col = st.columns(2)
            insert_clicked = insert_col.form_submit_button("INSERT", use_container_width=True, disabled=controls_disabled)
            update_clicked = update_col.form_submit_button("UPDATE", use_container_width=True, disabled=controls_disabled)

        if insert_clicked:
            st.session_state.selected_user_id = int(user_id)
            ok, data = call_api("POST", f"/user/{int(user_id)}/insert", {"name": name, "email": email, "status": status})
            st.session_state.last_action_feedback = (ok, data)
            fetch_ui_state.clear()
            st.rerun()

        if update_clicked:
            st.session_state.selected_user_id = int(user_id)
            ok, data = call_api("POST", f"/user/{int(user_id)}", {"name": name, "email": email, "status": status})
            st.session_state.last_action_feedback = (ok, data)
            fetch_ui_state.clear()
            st.rerun()

        if st.session_state.last_action_feedback is not None:
            ok, data = st.session_state.last_action_feedback
            if ok:
                st.success("Database action completed successfully.")
                st.json(data)
            else:
                st.warning(get_feedback_message(data, "Database action could not be completed right now."))

        st.subheader("Docker Control Panel")
        st.warning("These buttons run docker compose commands in the repository root. Review before using them.")
        st.checkbox(
            "I understand these actions can rebuild or stop the local stack.",
            key="docker_warning_ack",
        )
        start_col, stop_col, rebuild_col = st.columns(3)
        if start_col.button("Start System", use_container_width=True, disabled=not st.session_state.docker_warning_ack):
            ok, output = run_docker_command(["up", "--build", "-d"])
            (st.success if ok else st.error)("System start completed." if ok else "System start failed.")
            st.code(output or "No output")
        if stop_col.button("Stop System", use_container_width=True, disabled=not st.session_state.docker_warning_ack):
            ok, output = run_docker_command(["down"])
            (st.success if ok else st.error)("System stop completed." if ok else "System stop failed.")
            st.code(output or "No output")
        if rebuild_col.button("Rebuild System", use_container_width=True, disabled=not st.session_state.docker_warning_ack):
            ok, output = run_docker_command(["build"])
            (st.success if ok else st.error)("System rebuild completed." if ok else "System rebuild failed.")
            st.code(output or "No output")

    with right:
        st.subheader("Cache Observability Panel")
        explain(
            "Green means Redis currently holds the user key. Red means the key is absent or has been invalidated, so the next read will repopulate from MySQL.",
            explain_mode,
        )
        if controls_disabled:
            st.info(
                "Cache observability is temporarily unavailable while services are starting or a cache mode switch is in progress."
            )
        else:
            read_col, inspect_col = st.columns(2)
            if read_col.button("Read Through Cache", use_container_width=True, disabled=controls_disabled):
                ok, data = call_api("POST", f"/ui/cache/read/{st.session_state.selected_user_id}")
                if ok:
                    st.success(f"Cache {data['cache_result'].upper()} for user {st.session_state.selected_user_id}")
                    st.json(data["user"])
                    fetch_ui_state.clear()
                else:
                    st.warning(get_feedback_message(data, "Cache read is temporarily unavailable."))
            if inspect_col.button("Inspect Cache", use_container_width=True, disabled=controls_disabled):
                ok, data = call_api("GET", f"/cache/{st.session_state.selected_user_id}")
                if ok:
                    st.json(data)
                    fetch_ui_state.clear()
                else:
                    st.warning(get_feedback_message(data, "Cache inspection is temporarily unavailable."))

            render_cache_card(state.get("cache_state") if state else None, state.get("db_state") if state else None)

    st.subheader("CDC Pipeline Visualization")
    explain(
        "After a write hits MySQL, Debezium captures the binlog change, Kafka transports it, the CDC consumer handles it, and Redis invalidates the key so future reads are fresh.",
        explain_mode,
    )
    if controls_disabled:
        st.info("Pipeline visualization will resume automatically once backend services are ready.")
    else:
        render_pipeline(state.get("last_flow") if state else None)

    app_state = ensure_dict(state.get("state"), context="ui_state.state") if state else {}
    explainers = ensure_dict(state.get("explainers"), context="ui_state.explainers") if state else {}
    last_db_operation = ensure_dict(app_state.get("last_db_operation"), context="ui_state.state.last_db_operation")
    last_cdc_event = ensure_dict(app_state.get("last_cdc_event"), context="ui_state.state.last_cdc_event")

    summary_col_1, summary_col_2, summary_col_3 = st.columns(3)
    summary_col_1.metric("Cache mode", current_cache_mode)
    summary_col_2.metric("Last DB op", safe_get(last_db_operation, "operation", default="none"))
    summary_col_3.metric("Last CDC stage", safe_get(last_cdc_event, "stage", default="none"))

    if explain_mode and state:
        with st.expander("What is happening under the hood?"):
            st.write(safe_get(explainers, "cdc", default=""))
            st.write(safe_get(explainers, "redis_invalidation", default=""))
            st.write("A cache hit serves Redis immediately. A cache miss falls through to MySQL and repopulates Redis.")


try:
    run_app()
except Exception as exc:
    LOGGER.exception("ui_render_failed")
    st.error("Something went wrong while updating the system.")
    st.info(
        "Possible reasons:\n"
        "- UI state conflict\n"
        "- Backend not reachable\n"
        "- Invalid cache mode transition\n\n"
        "Please try:\n"
        "- Refreshing the page\n"
        "- Re-selecting the mode"
    )
    with st.expander("Show technical details"):
        st.code(str(exc))




