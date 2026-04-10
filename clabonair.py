#!/usr/bin/env python3
"""
clabonair.py - Deploy containerlab topologies on Nvidia Air

Reads a containerlab YAML topology, creates Ubuntu VMs on Nvidia Air with
matching links. Images are distributed peer-to-peer using an exponential
doubling tree (docker save/load over HTTP) — no dedicated registry VM needed.
Nodes install Docker + containerlab in parallel, then a doubling distribution
phase pre-loads container images before deploying single-node topologies.

Usage:
  python3 clabonair.py deploy -t 2node.clab.yml
  python3 clabonair.py deploy -t 2node.clab.yml --debug
  python3 clabonair.py destroy [--title TITLE] [--sim-id SIM_ID]
  python3 clabonair.py status  [--title TITLE] [--sim-id SIM_ID]
"""

import sys
import os
import socket
import time
import argparse
import json
import uuid
import base64
import re
import logging
import threading
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml
import paramiko
import websockets.sync.client as ws_sync

from air_sdk.v2 import AirApi

# Suppress noisy websockets keepalive errors on connection teardown
logging.getLogger("websockets").setLevel(logging.CRITICAL)
logging.getLogger("paramiko").setLevel(logging.CRITICAL)

# Module-level logger — configured by setup_debug_logging() when --debug is passed
log = logging.getLogger("clabonair")

SCRIPT_DIR = Path(__file__).resolve().parent

# --- Debug log helpers ---
_RE_B64_DECODE = re.compile(r"base64\.b64decode\('([A-Za-z0-9+/=]+)'\)")
_RE_B64_ECHO = re.compile(r"(echo -n ')([A-Za-z0-9+/=]{64,})(')")


def _sanitize_b64(cmd_str):
    """Replace base64 blobs in a command string with size placeholders for logging."""
    def _replace_decode(m):
        return f"base64.b64decode('<b64 ~{len(m.group(1)) * 3 // 4} bytes>')"
    def _replace_echo(m):
        return f"{m.group(1)}<b64 ~{len(m.group(2)) * 3 // 4} bytes>{m.group(3)}"
    s = _RE_B64_DECODE.sub(_replace_decode, cmd_str)
    s = _RE_B64_ECHO.sub(_replace_echo, s)
    return s


def setup_debug_logging():
    """Enable detailed debug logging to a file in the script's directory."""
    log_path = SCRIPT_DIR / "clabonair-debug.log"
    log.setLevel(logging.DEBUG)
    handler = logging.FileHandler(log_path, mode="w")
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(threadName)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    log.addHandler(handler)
    # Library loggers: critical only (avoids paramiko/websockets SSH banner noise)
    for lib_logger_name in ("paramiko", "websockets"):
        lib_logger = logging.getLogger(lib_logger_name)
        lib_logger.setLevel(logging.CRITICAL)
    log.debug("Debug logging started — log file: %s", log_path)
    print(f"[+] Debug logging enabled: {log_path}")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AIR_API_URL = "https://air.nvidia.com/api/"
AIR_WEB_URL = "https://air.nvidia.com"
WORK_DIR = Path.cwd()
VARS_FILE = WORK_DIR / "air.vars"

UBUNTU_OS = "generic/ubuntu2204"
UBUNTU_USER = "ubuntu"
UBUNTU_PASS = "nvidia"
VM_STORAGE = 10

# VM resource tiers by containerlab kind
VM_TIERS = {
    "nokia_srlinux": {"cpu": 3, "memory": 4096},
    "linux":         {"cpu": 1, "memory": 2048},
}
VM_TIER_DEFAULT = {"cpu": 3, "memory": 4096}

CLAB_CONTAINER_PREFIX = "clab"
SRL_IMAGE = "ghcr.io/nokia/srlinux"

MGMT_USER = "clabonair"
MGMT_PASS = "nvidia"
MGMT_SSH_PORT = 2222

VM_BOOT_TIMEOUT = 600
CMD_TIMEOUT = 300

DIST_HTTP_PORT = 8000
DOCKER_LOAD_TIMEOUT = 600  # 10 min for large images (2.5GB+)
MAX_CONSOLE_WORKERS = 8  # Limit concurrent console connections to avoid overwhelming Air
_cancel = threading.Event()  # Set on Ctrl+C to stop background threads

# Markers written to log files to signal phase completion
MARKER_INSTALL_DONE = "CLAB_INSTALL_DONE"
MARKER_DEPLOY_DONE = "CLAB_SETUP_SUCCESS"
MARKER_FORWARD_DONE = "FORWARDING_SETUP_DONE"
MARKER_DIST_READY = "CLAB_DIST_READY"
MARKER_DIST_FAILED = "CLAB_DIST_FAILED"

# Paths on the VM filesystem (use .format(node=name) to expand)
INSTALL_LOG_PATH = "/tmp/clab-install-{node}.log"
INSTALL_SCRIPT_PATH = "/tmp/clab-install-{node}.sh"
WORKER_LOG_PATH = "/tmp/clab-setup-{node}.log"
WORKER_SCRIPT_PATH = "/tmp/clab-setup-{node}.sh"
FORWARDING_SCRIPT_PATH = "/tmp/clab-forwarding-{node}.sh"
DIST_LOG_PATH = "/tmp/clab-dist-{node}.log"
DIST_SCRIPT_PATH = "/tmp/clab-dist-{node}.sh"
IMAGE_TAR_PATH = "/tmp/clab-image.tar.gz"
STARTUP_CFG_PATH = "/tmp/srl-config-{node}.cfg"


# ---------------------------------------------------------------------------
# Deploy UI — spinner + per-node status display
# ---------------------------------------------------------------------------

class DeployUI:
    """Thread-safe terminal UI with spinning cursor and per-node status lines.

    Usage:
        ui = DeployUI()
        ui.begin(["spine1", "spine2", "leaf1"])   # start spinner
        # ... worker threads call ui.mark_done("spine1") etc.
        ui.stop()                                  # stop spinner, final render
    """

    SPINNER_CHARS = ["|", "/", "-", "\\"]

    def __init__(self):
        self._lock = threading.Lock()
        self._nodes = {}          # {name: {"status","elapsed","start"}}
        self._node_order = []
        self._spinner_idx = 0
        self._spinner_thread = None
        self._running = False
        self._rendered_lines = 0
        self._is_tty = sys.stdout.isatty()
        self._max_name_len = 0

    def begin(self, node_names, start_all=True):
        """Initialize per-node tracking and start the spinner.

        If start_all is False, nodes begin in 'waiting' state and their
        timer only starts when mark_start(name) is called.
        """
        self.stop()  # stop any previous spinner
        now = time.time() if start_all else None
        with self._lock:
            self._node_order = list(node_names)
            self._max_name_len = max((len(n) for n in node_names), default=0)
            self._nodes = {
                n: {"status": "pending", "elapsed": 0.0, "start": now}
                for n in node_names
            }
            self._rendered_lines = 0
            self._spinner_idx = 0
        if self._is_tty:
            # Print initial blank lines so _render can overwrite them
            for _ in node_names:
                sys.stdout.write("\n")
            # Move cursor back up to the start of the block and save position.
            # Using save/restore prevents Enter key presses from duplicating
            # the list (relative \033[NA would be thrown off by injected newlines).
            sys.stdout.write(f"\033[{len(node_names)}A")
            sys.stdout.write("\033[s")  # save cursor position
            sys.stdout.flush()
            with self._lock:
                self._rendered_lines = len(node_names)
            self._running = True
            self._spinner_thread = threading.Thread(
                target=self._spin_loop, daemon=True)
            self._spinner_thread.start()

    def mark_start(self, name):
        """Record that work has actually started on a node (thread-safe).

        Sets the per-node timer start. Call this when a node begins work
        in phases where nodes don't all start simultaneously (e.g. tiered
        image distribution).
        """
        with self._lock:
            if name in self._nodes and self._nodes[name]["start"] is None:
                self._nodes[name]["start"] = time.time()

    def mark_done(self, name):
        """Mark a node as done (thread-safe)."""
        with self._lock:
            if name not in self._nodes:
                return
            info = self._nodes[name]
            if info["status"] != "done":
                info["status"] = "done"
                start = info["start"] if info["start"] is not None else time.time()
                info["elapsed"] = time.time() - start
        if not self._is_tty:
            # Non-TTY: print a line immediately
            with self._lock:
                elapsed = self._nodes[name]["elapsed"]
            print(f"      {name:<{self._max_name_len}}  Done! ({elapsed:.0f}s)",
                  flush=True)

    def mark_error(self, name, msg=""):
        """Mark a node as failed (thread-safe)."""
        with self._lock:
            if name not in self._nodes:
                return
            info = self._nodes[name]
            info["status"] = "error"
            start = info["start"] if info["start"] is not None else time.time()
            info["elapsed"] = time.time() - start
        if not self._is_tty:
            print(f"      {name:<{self._max_name_len}}  FAILED ({msg})",
                  flush=True)

    def stop(self):
        """Stop the spinner and do a final render."""
        if not self._running:
            return
        self._running = False
        if self._spinner_thread:
            self._spinner_thread.join(timeout=2)
            self._spinner_thread = None
        if self._is_tty:
            with self._lock:
                self._render()

    def _spin_loop(self):
        while self._running:
            time.sleep(0.15)
            if not self._running:
                break
            with self._lock:
                self._spinner_idx = (self._spinner_idx + 1) % 4
                self._render()

    def _render(self):
        """Overwrite the node status block using ANSI escape codes.
        Must be called while holding self._lock."""
        if not self._is_tty or not self._node_order:
            return
        # Restore cursor to saved position (immune to external newlines)
        sys.stdout.write("\033[u")
        lines = []
        for name in self._node_order:
            info = self._nodes[name]
            padded = f"{name:<{self._max_name_len}}"
            if info["status"] == "done":
                lines.append(f"      {padded}  Done! ({info['elapsed']:.0f}s)")
            elif info["status"] == "error":
                lines.append(f"      {padded}  FAILED")
            elif info["start"] is None:
                lines.append(f"      {padded}  Waiting")
            else:
                char = self.SPINNER_CHARS[self._spinner_idx]
                lines.append(f"      {padded}  {char}")
        output = "\n".join(f"\033[K{line}" for line in lines) + "\n"
        sys.stdout.write(output)
        sys.stdout.flush()
        self._rendered_lines = len(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VARS_TEMPLATE = """\
###########################################################
# Nvidia Air credentials                                  #
# Get your API token from https://air.nvidia.com/settings #
###########################################################

EMAIL=
API_TOKEN=

# Optional:
ORGANIZATION=
"""


def load_env(path: Path) -> dict:
    log.debug("Loading env vars from %s", path)
    env = {}
    if not path.exists():
        path.write_text(VARS_TEMPLATE)
        print(f"[!] Created template vars file: {path}", file=sys.stderr)
        print(f"    Fill in your Nvidia Air credentials and run again.", file=sys.stderr)
        print(f"    Get an API token from https://air.nvidia.com/account/api-tokens",
              file=sys.stderr)
        sys.exit(1)
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                env[key.strip()] = value.strip()
    return env


def connect(cfg: dict) -> AirApi:
    username = cfg.get("AIR_USERNAME") or cfg.get("USERNAME") or cfg.get("EMAIL")
    password = cfg.get("AIR_PASSWORD") or cfg.get("PASSWORD")
    api_token = cfg.get("API_TOKEN")

    log.debug("Connecting to Air API at %s (user=%s)", AIR_API_URL, username)
    print(f"[+] Connecting to Nvidia AIR ...")

    if username and password:
        api = AirApi(api_url=AIR_API_URL, username=username, password=password)
    elif username and api_token:
        api = AirApi(api_url=AIR_API_URL, username=username, password=api_token)
    else:
        print("[-] No credentials. Set EMAIL+API_TOKEN or AIR_USERNAME+AIR_PASSWORD.",
              file=sys.stderr)
        sys.exit(1)

    try:
        list(api.simulations.list())
    except Exception as exc:
        if "403" in str(exc):
            print(f"[-] Authentication failed: {exc}", file=sys.stderr)
        else:
            print(f"[-] API connection error: {exc}", file=sys.stderr)
        sys.exit(1)

    print("[+] Authenticated.")
    return api


def resolve_organization(api: AirApi, org_value: str) -> str:
    """Resolve an organization name or UUID to its UUID.

    If *org_value* is already a valid UUID it is returned as-is.
    Otherwise it is treated as an organization name and looked up via the API.
    """
    try:
        uuid.UUID(org_value)
        return org_value          # already a UUID
    except ValueError:
        pass                      # not a UUID – treat as name

    for org in api.organizations.list():
        if org.name == org_value:
            log.debug("Resolved organization name '%s' -> %s", org_value, org.id)
            return org.id

    print(f"[-] Organization '{org_value}' not found. "
          f"Check the name or use the UUID instead.", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Containerlab YAML Parser
# ---------------------------------------------------------------------------

def parse_clab_yaml(clab_path: Path) -> dict:
    log.debug("Parsing clab YAML: %s", clab_path)
    with open(clab_path) as fh:
        data = yaml.safe_load(fh)

    topo = data.get("topology", {})
    lab_name = data.get("name", clab_path.stem)
    clab_dir = clab_path.resolve().parent

    # Resolve defaults and kind-level defaults (containerlab resolution order)
    defaults = topo.get("defaults", {})
    kinds = topo.get("kinds", {})

    nodes = {}
    for name, ndef in topo.get("nodes", {}).items():
        # Resolve kind: node > defaults
        kind = ndef.get("kind", defaults.get("kind", "nokia_srlinux"))
        # Resolve image: node > kinds[kind] > defaults > hardcoded
        kind_defaults = kinds.get(kind, {})
        image = ndef.get("image", kind_defaults.get("image", defaults.get("image", SRL_IMAGE)))
        # Resolve type: node > kinds[kind] > defaults
        node_type = ndef.get("type", kind_defaults.get("type", defaults.get("type")))

        cfg_path = None
        # Resolve startup-config: node > kinds[kind] > defaults
        startup_cfg = (ndef.get("startup-config")
                       or kind_defaults.get("startup-config")
                       or defaults.get("startup-config"))
        if startup_cfg:
            cfg_path = (clab_dir / startup_cfg).resolve()
            if not cfg_path.is_file():
                print(f"[!] Warning: startup-config not found: {cfg_path}")
                cfg_path = None
        nodes[name] = {
            "kind": kind,
            "image": image,
            "type": node_type,
            "startup_config": cfg_path,
        }

    links = []
    for link_def in topo.get("links", []):
        eps = link_def.get("endpoints", [])
        if len(eps) == 2:
            parsed = [tuple(ep.split(":", 1)) for ep in eps]
            links.append({"endpoints": parsed})

    return {"name": lab_name, "nodes": nodes, "links": links}


# ---------------------------------------------------------------------------
# Air Topology Builder
# ---------------------------------------------------------------------------

def _make_vm_spec(cpu, memory, x, y):
    """Build an Air VM specification dict."""
    return {
        "nic_model": "virtio",
        "cpu": cpu,
        "memory": memory,
        "storage": VM_STORAGE,
        "positioning": {"x": x, "y": y},
        "os": UBUNTU_OS,
        "features": {"uefi": False, "tpm": False},
        "pxehost": False,
        "secureboot": False,
        "oob": False,
        "emulation_type": None,
        "network_pci": {},
        "cpu_options": ["ssse3"],
        "storage_pci": {},
    }


def build_air_topology(clab_data: dict) -> tuple:
    log.debug("Building Air topology: %d nodes, %d links",
              len(clab_data["nodes"]), len(clab_data["links"]))
    nodes = clab_data["nodes"]
    links = clab_data["links"]

    air_nodes = {}
    node_vm_map = {}

    x_start, y_start, x_step = 275, 275, 300
    for i, (name, ndef) in enumerate(nodes.items()):
        tier = VM_TIERS.get(ndef["kind"], VM_TIER_DEFAULT)
        air_nodes[name] = _make_vm_spec(
            tier["cpu"], tier["memory"], x_start + i * x_step, y_start)
        node_vm_map[name] = {
            "vm_name": name,
            "macvlan_links": [],
            "kind": ndef["kind"],
            "image": ndef["image"],
            "type": ndef.get("type"),
            "startup_config": ndef.get("startup_config"),
        }

    node_eth_counter = defaultdict(int)
    air_links = []

    for link in links:
        (nodeA, ifaceA), (nodeB, ifaceB) = link["endpoints"]

        node_eth_counter[nodeA] += 1
        ethA = f"eth{node_eth_counter[nodeA]}"
        node_vm_map[nodeA]["macvlan_links"].append((ifaceA, ethA))

        node_eth_counter[nodeB] += 1
        ethB = f"eth{node_eth_counter[nodeB]}"
        node_vm_map[nodeB]["macvlan_links"].append((ifaceB, ethB))

        air_links.append([
            {"interface": ethA, "node": nodeA, "network_pci": None},
            {"interface": ethB, "node": nodeB, "network_pci": None},
        ])

    air_content = {
        "nodes": air_nodes,
        "links": air_links,
        "oob": {
            "nodes": {
                "oob-mgmt-server": {
                    "cpu": 10, "memory": 8192, "storage": 10,
                    "positioning": {"x": 800, "y": 0},
                },
                "oob-mgmt-switch": {
                    "cpu": 1, "memory": 8192, "storage": 10,
                    "positioning": {"x": 400, "y": 0},
                },
            },
            "enable_dhcp": True,
        },
        "netq": False,
    }

    return air_content, node_vm_map


# ---------------------------------------------------------------------------
# VM Access via WebSocket Console
# ---------------------------------------------------------------------------

def _get_node_pk(node_ref):
    """Extract node primary key from a FK reference."""
    if hasattr(node_ref, 'id'):
        return str(node_ref.id)
    if hasattr(node_ref, '__pk__'):
        return str(node_ref.__pk__)
    return str(node_ref)


def _ws_bridge(console_url, bridge_sock):
    """Thread: relay data between WebSocket (base64 subprotocol) and a socket."""
    try:
        ws = ws_sync.connect(
            console_url, subprotocols=["base64"], close_timeout=5,
            ping_interval=20, ping_timeout=20,
        )

        def reader():
            try:
                while True:
                    msg = ws.recv()
                    bridge_sock.sendall(base64.b64decode(msg))
            except Exception:
                bridge_sock.close()

        threading.Thread(target=reader, daemon=True).start()

        while True:
            data = bridge_sock.recv(4096)
            if not data:
                break
            ws.send(base64.b64encode(data).decode())
    except Exception:
        pass


def _read_shell_until(channel, pattern, timeout=30):
    """Read from a shell channel until pattern appears. Returns accumulated text."""
    buf = ""
    deadline = time.time() + timeout
    channel.settimeout(2)
    while time.time() < deadline:
        try:
            data = channel.recv(4096).decode("utf-8", errors="replace")
            buf += data
            if pattern in buf:
                return buf
        except socket.timeout:
            continue
    raise TimeoutError(f"Timed out waiting for '{pattern}'. Tail: ...{buf[-200:]}")


class ConsoleClient:
    """Wraps a WebSocket console connection to an Ubuntu VM.
    Provides run() and write_file() methods over the PTY channel,
    using unique markers to delimit command output and exit codes."""

    def __init__(self, node_name, console_url, console_user, console_pass,
                 vm_user=None, vm_pass=None, connect_timeout=None,
                 probe_stale=False):
        self.node_name = node_name
        self._param_sock = None
        self._transport = None
        self._channel = None
        self._console_url = console_url
        self._console_user = console_user
        self._console_pass = console_pass
        self._vm_user = vm_user or UBUNTU_USER
        self._vm_pass = vm_pass or UBUNTU_PASS
        self._connect_timeout = connect_timeout
        self._probe_stale = probe_stale

    def __enter__(self):
        self.connect(timeout=self._connect_timeout or VM_BOOT_TIMEOUT,
                     probe_stale=self._probe_stale)
        return self

    def __exit__(self, *exc_info):
        self.close()

    def connect(self, timeout=VM_BOOT_TIMEOUT, probe_stale=False):
        """Open WebSocket console, SSH transport, login to Ubuntu shell.
        If probe_stale=True, detect and handle stale shell sessions
        (needed for Phase 5 after Phase 4 polling leaves shells active)."""
        deadline = time.time() + timeout
        t0 = time.time()
        attempt = 0

        while time.time() < deadline:
            attempt += 1
            try:
                self._param_sock, bridge_sock = socket.socketpair()
                threading.Thread(
                    target=_ws_bridge,
                    args=(self._console_url, bridge_sock),
                    daemon=True,
                ).start()
                time.sleep(1)

                self._transport = paramiko.Transport(self._param_sock)
                self._transport.start_client()
                self._transport.auth_password(self._console_user, self._console_pass)

                self._channel = self._transport.open_session()
                self._channel.get_pty(term="xterm", width=200, height=50)
                self._channel.invoke_shell()

                if probe_stale:
                    # Probe console state: check for login prompt vs
                    # stale shell session (e.g. after Phase 4 polling)
                    self._channel.send("\n")
                    self._channel.settimeout(5)
                    probe_buf = ""
                    probe_deadline = time.time() + 20
                    while time.time() < probe_deadline:
                        try:
                            data = self._channel.recv(4096).decode("utf-8", errors="replace")
                            probe_buf += data
                            if "login:" in probe_buf:
                                break
                            if "$ " in probe_buf or "# " in probe_buf:
                                # Stale shell session — terminate it
                                self._channel.send("exit\n")
                                time.sleep(2)
                                self._channel.send("\n")
                                probe_buf = ""
                                # Now wait for login prompt after logout
                                _read_shell_until(self._channel, "login:", timeout=20)
                                break
                        except socket.timeout:
                            continue
                    else:
                        # Neither prompt found — foreground process may be running
                        # (e.g. docker load still active from a previous phase).
                        # Send Ctrl+C to interrupt any foreground process, then
                        # send newlines to elicit a prompt.  Do NOT send "exit"
                        # because if the console is actually at a login prompt
                        # the word "exit" gets typed as a username, corrupting
                        # the subsequent credential flow.
                        self._channel.send("\x03")
                        time.sleep(1)
                        # Drain anything produced by Ctrl+C
                        self._channel.settimeout(2)
                        drain_buf = ""
                        try:
                            while True:
                                drain_buf += self._channel.recv(4096).decode(
                                    "utf-8", errors="replace")
                        except socket.timeout:
                            pass
                        # If we're in a shell, logout first
                        if "$ " in drain_buf or "# " in drain_buf:
                            self._channel.send("exit\n")
                            time.sleep(2)
                        # Send newline and wait for login prompt
                        self._channel.send("\n")
                        _read_shell_until(self._channel, "login:", timeout=20)
                else:
                    # Simple approach: just wait for login prompt
                    # (safe during boot — ignores kernel/systemd output)
                    self._channel.send("\n")
                    _read_shell_until(self._channel, "login:", timeout=30)

                # Brief drain to clear any residual data from probe/wait,
                # then send a clean newline so we're at a fresh login prompt
                self._channel.settimeout(1)
                try:
                    while True:
                        self._channel.recv(4096)
                except socket.timeout:
                    pass
                time.sleep(0.3)
                self._channel.send("\n")
                _read_shell_until(self._channel, "login:", timeout=15)

                time.sleep(0.5)
                self._channel.send(self._vm_user + "\n")
                _read_shell_until(self._channel, "assword:", timeout=15)
                time.sleep(0.3)
                self._channel.send(self._vm_pass + "\n")

                # Wait for shell prompt ($ or #)
                buf = _read_shell_until(self._channel, "$", timeout=30)

                # Drain any remaining MOTD output
                self._channel.settimeout(2)
                try:
                    while True:
                        self._channel.recv(4096)
                except socket.timeout:
                    pass

                # Disable terminal echo and paging for clean output
                self._channel.send("export TERM=dumb; stty -echo\n")
                time.sleep(0.5)
                try:
                    while True:
                        self._channel.recv(4096)
                except socket.timeout:
                    pass

                log.debug("[%s] connected (attempt %d, %.1fs)", self.node_name, attempt, time.time() - t0)
                return self

            except Exception as exc:
                if attempt % 5 == 0:
                    log.debug("[%s] connect attempt %d failed: %s", self.node_name, attempt, exc)
                self._cleanup_partial()
                remaining = deadline - time.time()
                if remaining <= 15:
                    raise TimeoutError(
                        f"[{self.node_name}] Console not reachable after {timeout}s: {exc}")
                if attempt % 3 == 0:
                    print(f"    [{self.node_name}] Console not ready "
                          f"(attempt {attempt}), retrying ...")
                time.sleep(10)

        raise TimeoutError(f"[{self.node_name}] Console not reachable after {timeout}s")

    def _cleanup_partial(self):
        for obj in (self._channel, self._transport, self._param_sock):
            if obj:
                try:
                    obj.close()
                except Exception:
                    pass
        self._channel = self._transport = self._param_sock = None

    def close(self):
        """Clean up shell session on serial console, then close connections.
        Sends Ctrl+C (interrupt any running command) + exit (terminate shell)
        so that agetty respawns a clean login prompt on ttyS0."""
        if self._channel:
            try:
                self._channel.send("\x03\n")   # Ctrl+C to interrupt foreground process
                time.sleep(0.3)
                self._channel.send("exit\n")   # Logout the shell session
                time.sleep(0.5)
            except Exception:
                pass
        self._cleanup_partial()

    def run(self, cmd, timeout=CMD_TIMEOUT):
        """Run a shell command, return (exit_code, stdout).
        Uses unique start/end markers to cleanly delimit output."""
        t0_run = time.time()
        log.debug("[%s] run(timeout=%s): %s", self.node_name, timeout, _sanitize_b64(cmd[:200]))
        tag = uuid.uuid4().hex[:12]
        start_marker = f"__START_{tag}__"
        end_marker = f"__END_{tag}__"

        # Wrapped: print start marker, run command, print end marker with exit code
        wrapped = (
            f"echo {start_marker}; {cmd}; echo {end_marker}$?\n"
        )

        # Drain any pending output
        self._channel.settimeout(0.5)
        try:
            while True:
                self._channel.recv(4096)
        except (socket.timeout, OSError):
            pass

        # Throttle large commands to avoid overwhelming the PTY input buffer.
        # Serial consoles have limited buffering (~4KB); sending a big heredoc
        # in one shot causes data loss.  Send line-by-line with small delays.
        raw = wrapped.encode("utf-8") if isinstance(wrapped, str) else wrapped
        SEND_CHUNK = 256          # bytes per send – well within PTY buffer
        SEND_DELAY = 0.03         # seconds between chunks
        for i in range(0, len(raw), SEND_CHUNK):
            self._channel.sendall(raw[i:i + SEND_CHUNK])
            if i + SEND_CHUNK < len(raw):
                time.sleep(SEND_DELAY)

        # Read until we see the end marker
        buf = ""
        deadline = time.time() + timeout
        self._channel.settimeout(2)
        while time.time() < deadline:
            try:
                data = self._channel.recv(8192)
                if not data:
                    raise ConnectionError(
                        f"[{self.node_name}] Console connection closed during command")
                buf += data.decode("utf-8", errors="replace")
                # Check for end marker followed by a digit (exit code).
                # When terminal echo is ON, the echoed command contains
                # __END_tag__$? ($ is not a digit); the real output has
                # __END_tag__0 (digit). rfind picks the last occurrence.
                em_pos = buf.rfind(end_marker)
                if em_pos >= 0:
                    after_pos = em_pos + len(end_marker)
                    if after_pos < len(buf) and buf[after_pos].isdigit():
                        break
            except socket.timeout:
                continue
            except (ConnectionError, OSError):
                raise
        else:
            # Cancel the stuck command so the PTY is clean for retries
            try:
                self._channel.send("\x03\n")
                time.sleep(0.5)
                self._channel.settimeout(0.5)
                try:
                    while True:
                        self._channel.recv(4096)
                except (socket.timeout, OSError):
                    pass
            except Exception:
                pass
            log.debug("[%s] run() TIMEOUT after %ds, buf=%d bytes: %s",
                      self.node_name, timeout, len(buf), buf[-500:] if buf else "(empty)")
            raise TimeoutError(f"[{self.node_name}] Command timed out: {cmd[:60]}...")

        # Extract output between start and end markers
        # Find start marker
        start_idx = buf.rfind(start_marker)
        if start_idx >= 0:
            after_start = buf[start_idx + len(start_marker):]
        else:
            after_start = buf

        # Find end marker and extract exit code
        end_idx = after_start.find(end_marker)
        if end_idx >= 0:
            stdout = after_start[:end_idx]
            rc_str = after_start[end_idx + len(end_marker):].strip().split("\n")[0].strip()
        else:
            stdout = after_start
            rc_str = "1"

        rc = int(rc_str) if rc_str.isdigit() else 1
        # Strip leading/trailing whitespace from output
        stdout = stdout.strip("\r\n")

        log.debug("[%s] run() rc=%d, %d bytes, %.1fs", self.node_name, rc, len(stdout), time.time() - t0_run)
        return rc, stdout

    def send_cmd(self, cmd):
        """Send a command without waiting for output (fire-and-forget)."""
        self._channel.send(cmd + "\n")
        time.sleep(1)

    def write_file(self, remote_path, content, retries=2):
        """Write content to a file on the VM via base64-encoded chunks.
        For small payloads (≤4KB b64) uses a single python3 -c call.
        For larger payloads, writes b64 chunks to a temp file then decodes.
        Retries on timeout/connection errors."""
        t0_wf = time.time()
        log.debug("[%s] write_file(%s, %d bytes, retries=%d)",
                  self.node_name, remote_path, len(content), retries)
        b64 = base64.b64encode(content.encode()).decode()
        last_exc = None
        for attempt in range(1 + retries):
            if attempt > 0:
                time.sleep(3)
            try:
                if len(b64) <= 4096:
                    # Single command — no heredoc, no multi-line PTY issues
                    cmd = (
                        f"sudo python3 -c \"import base64;"
                        f"open('{remote_path}','wb').write("
                        f"base64.b64decode('{b64}'))\""
                    )
                    self.run(cmd, timeout=90)
                else:
                    # Large file: write b64 to temp file in chunks, then decode
                    tmp = f"/tmp/.b64_{uuid.uuid4().hex[:8]}"
                    CHUNK = 3072  # ~3KB per echo — safe for PTY buffer
                    for i in range(0, len(b64), CHUNK):
                        chunk = b64[i:i + CHUNK]
                        op = ">" if i == 0 else ">>"
                        self.run(f"echo -n '{chunk}' {op} {tmp}", timeout=60)
                    self.run(
                        f"base64 -d {tmp} | sudo tee {remote_path} > /dev/null"
                        f" && rm -f {tmp}",
                        timeout=60,
                    )
                # Verify file was written correctly.
                # Use a unique FSIZE= prefix so the regex is unambiguous even
                # when terminal echo is ON (probe_stale can leave echo active,
                # causing echoed file paths with digits to pollute the output).
                rc, out = self.run(
                    f"echo FSIZE=$(sudo stat -c %s {remote_path} 2>/dev/null || echo MISSING)",
                    timeout=15,
                )
                expected_size = len(content.encode("utf-8"))
                match = re.search(r'FSIZE=(\d+)', out)
                actual_size = int(match.group(1)) if match else -1
                if actual_size != expected_size:
                    raise ConnectionError(
                        f"[{self.node_name}] File size mismatch for {remote_path}: "
                        f"expected {expected_size}, got {actual_size}")
                log.debug("[%s] write_file(%s) done, %.1fs", self.node_name, remote_path, time.time() - t0_wf)
                return  # success
            except (TimeoutError, ConnectionError) as exc:
                last_exc = exc
                if attempt < retries:
                    print(f"    [{self.node_name}] write_file retry for {remote_path}")
        raise last_exc


def get_console_info(api, sim, node_names):
    """Get console access info for specified nodes.
    Returns {name: (console_url, console_user, console_pass)}."""
    log.debug("get_console_info: fetching for %s", node_names)
    air_nodes = list(api.nodes.list(simulation=sim))
    result = {}
    for node in air_nodes:
        if node.name not in node_names:
            continue
        console_url = getattr(node, "console_url", None)
        console_user = getattr(node, "console_username", None)
        console_pass = getattr(node, "console_password", None)
        if console_url and console_user:
            result[node.name] = (console_url, console_user, console_pass)
        else:
            print(f"    [!] No console access for {node.name}")
    return result


# ---------------------------------------------------------------------------
# VM Setup
# ---------------------------------------------------------------------------

def generate_single_node_clab_yaml(node_name, kind, image, macvlan_links,
                                   startup_config_path=None, node_type=None):
    links_yaml = ""
    for container_iface, vm_eth in macvlan_links:
        links_yaml += f'    - endpoints: ["{node_name}:{container_iface}", "macvlan:{vm_eth}"]\n'

    node_extras = ""
    if node_type:
        node_extras += f"\n      type: {node_type}"
    if startup_config_path:
        node_extras += f"\n      startup-config: {startup_config_path}"

    return f"""name: {node_name}
topology:
  nodes:
    {node_name}:
      kind: {kind}
      image: {image}{node_extras}
  links:
{links_yaml}"""


def generate_worker_install_script(node_name):
    """Generate phase-1 node script: wait for internet, install Docker + containerlab."""
    return f"""#!/bin/bash
set -euo pipefail
exec &> >(tee -a {INSTALL_LOG_PATH.format(node=node_name)})
set -x

# 1. Wait for internet
for i in $(seq 1 30); do
  ping -c 1 -W 3 8.8.8.8 && break
  [ $((i % 5)) -eq 0 ] && sudo dhclient eth0 2>/dev/null || true
  sleep 10
done

# 2. Install Docker (minimal, ephemeral VMs)
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" > /etc/apt/sources.list.d/docker.list
for attempt in 1 2 3; do
  if apt-get -qq update && apt-get -y -qq install docker-ce docker-ce-cli containerd.io; then
    break
  fi
  echo "Docker install attempt $attempt failed, retrying in 10s ..."
  sleep 10
done

# 3. Install containerlab via .deb package
LATEST=$(curl -sL -o /dev/null -w '%{{url_effective}}' https://github.com/srl-labs/containerlab/releases/latest | sed 's|.*/v||')
for attempt in 1 2 3; do
  if curl -sL -o /tmp/clab.deb "https://github.com/srl-labs/containerlab/releases/download/v${{LATEST}}/containerlab_${{LATEST}}_linux_amd64.deb" && sudo dpkg -i /tmp/clab.deb; then
    break
  fi
  echo "Containerlab install attempt $attempt failed, retrying in 10s ..."
  sleep 10
done

# 4. Verify Docker + containerlab are installed
command -v docker && docker --version
command -v containerlab && containerlab version

# 5. Write completion marker
echo "{MARKER_INSTALL_DONE}" >> {INSTALL_LOG_PATH.format(node=node_name)}
"""


def generate_seed_dist_script(node_name, image, has_children):
    """Generate Phase 2 tier-0 script: pull image, save to tar, optionally serve via HTTP."""
    serve_block = ""
    if has_children:
        serve_block = f"""
# 3. Serve image via HTTP for downstream nodes
cd /tmp && python3 -m http.server {DIST_HTTP_PORT} &
sleep 1
"""
    return f"""#!/bin/bash
set -euo pipefail
exec &> >(tee -a {DIST_LOG_PATH.format(node=node_name)})
set -x

# 1. Pull image (retry up to 3 times)
PULL_OK=0
for attempt in 1 2 3; do
  if sudo docker pull {image}; then
    PULL_OK=1
    break
  fi
  echo "Pull attempt $attempt failed, retrying in 15s ..."
  sleep 15
done
if [ "$PULL_OK" -ne 1 ]; then
  echo "{MARKER_DIST_FAILED}" >> {DIST_LOG_PATH.format(node=node_name)}
  exit 1
fi

# 2. Save and compress
sudo docker save {image} | gzip > {IMAGE_TAR_PATH}
{serve_block}
echo "{MARKER_DIST_READY}" >> {DIST_LOG_PATH.format(node=node_name)}
"""


def generate_worker_deploy_script(node_name, node_info):
    """Generate deploy script: bring up interfaces, deploy clab.
    Images are already pre-loaded by Phase 2 (peer distribution).
    Note: startup config is written as a separate file via ConsoleClient.write_file()
    before this script runs, to avoid heredoc delimiter collisions."""
    image = node_info["image"]

    iface_cmds = "\n".join(
        f"sudo ip link set {vm_eth} up mtu 9500"
        for _, vm_eth in node_info["macvlan_links"]
    )

    cfg_path = node_info.get("startup_config")
    clab_yaml = generate_single_node_clab_yaml(
        node_name, node_info["kind"], image, node_info["macvlan_links"],
        startup_config_path=STARTUP_CFG_PATH.format(node=node_name) if cfg_path else None,
        node_type=node_info.get("type"),
    )

    return f"""#!/bin/bash
set -euo pipefail
exec &> >(tee -a {WORKER_LOG_PATH.format(node=node_name)})
set -x

# 0. Verify Docker is installed and running
if ! command -v docker &>/dev/null; then
    echo "ERROR: Docker is not installed. Install script may have failed."
    exit 1
fi
if ! sudo systemctl is-active --quiet docker; then
    echo "WARNING: Docker not running, attempting to start..."
    sudo systemctl start docker || {{ echo "ERROR: Cannot start Docker"; exit 1; }}
fi

# 1. Bring up VM data interfaces
{iface_cmds}

# 2. Write single-node clab YAML
cat <<'CLAB' > /tmp/clab-{node_name}.yml
{clab_yaml}
CLAB

# 3. Deploy containerlab (image already local from Phase 2, pushes config automatically)
sudo containerlab deploy -t /tmp/clab-{node_name}.yml --reconfigure 2>&1

# 4. Write completion marker
echo "{MARKER_DEPLOY_DONE}" >> {WORKER_LOG_PATH.format(node=node_name)}
"""



def generate_forwarding_script(node_name, kind="nokia_srlinux"):
    """Generate Phase 5 script: create mgmt user, console forwarding, SSH port 22 -> container mgmt.
    Works for both SRL and linux kinds — SRL exits on missing mgmt IP, linux guards with if-check."""
    is_linux = kind == "linux"
    ip_var = "CONTAINER_IP" if is_linux else "SRL_MGMT_IP"
    ip_label = "IP" if is_linux else "mgmt IP"

    # SRL: hard-fail if no mgmt IP; linux: guard SSH forwarding with if-check
    if is_linux:
        ip_check = f'echo "Container: $CONTAINER, {ip_label}: ${ip_var}"'
        ssh_fwd = f"""if [ -n "${ip_var}" ]; then
    sysctl -w net.ipv4.ip_forward=1
    iptables -t nat -A PREROUTING -p tcp --dport 22 -j DNAT --to-destination ${{{ip_var}}}:22
    iptables -A FORWARD -p tcp -d ${{{ip_var}}} --dport 22 -j ACCEPT
    iptables -t nat -A POSTROUTING -j MASQUERADE
fi"""
    else:
        ip_check = f"""if [ -z "${ip_var}" ]; then
    echo "ERROR: Could not determine SRL management IP"
    exit 1
fi
echo "Container: $CONTAINER, {ip_label}: ${ip_var}" """
        ssh_fwd = f"""sysctl -w net.ipv4.ip_forward=1
iptables -t nat -A PREROUTING -p tcp --dport 22 -j DNAT --to-destination ${{{ip_var}}}:22
iptables -A FORWARD -p tcp -d ${{{ip_var}}} --dport 22 -j ACCEPT
iptables -t nat -A POSTROUTING -j MASQUERADE"""

    return f"""#!/bin/bash
set -euo pipefail

# Fix hostname resolution — without this, every command stalls ~10s on DNS
echo "127.0.1.1 $(hostname)" >> /etc/hosts

# Find clab container name and management IP
CONTAINER=$(docker ps --format '{{{{.Names}}}}' --filter "name=clab-" | head -1)
if [ -z "$CONTAINER" ]; then
    echo "ERROR: No clab container found"
    exit 1
fi
{ip_var}=$(docker inspect -f '{{{{range .NetworkSettings.Networks}}}}{{{{.IPAddress}}}}{{{{end}}}}' "$CONTAINER")
{ip_check}

# 1. Create management user for script access (bash shell, sudo)
useradd -m -s /bin/bash -G sudo {MGMT_USER} 2>/dev/null || true
echo "{MGMT_USER}:{MGMT_PASS}" | chpasswd

# 2. Move sshd to port {MGMT_SSH_PORT} for management access
sed -i 's/^#\\?Port 22$/Port {MGMT_SSH_PORT}/' /etc/ssh/sshd_config
grep -q "^Port {MGMT_SSH_PORT}" /etc/ssh/sshd_config || echo "Port {MGMT_SSH_PORT}" >> /etc/ssh/sshd_config
systemctl restart sshd

# 3. Set up SSH forwarding: port 22 -> container
{ssh_fwd}

# 4. Configure console auto-login (skip ubuntu credential prompt)
mkdir -p /etc/systemd/system/serial-getty@ttyS0.service.d
cat <<'AUTOLOGIN' > /etc/systemd/system/serial-getty@ttyS0.service.d/autologin.conf
[Service]
ExecStart=
ExecStart=-/sbin/agetty --autologin ubuntu --noclear %I $TERM
AUTOLOGIN
systemctl daemon-reload

echo "{MARKER_FORWARD_DONE}"
"""


def setup_workers_install(console_info, node_vm_map):
    """Connect to all nodes via WebSocket console in parallel, write and launch
    the install script (Docker + containerlab). Returns {name: ip} dict."""
    log.debug("setup_workers_install: %d nodes", len(node_vm_map))

    def _install_worker(name, info):
        """Returns (name, ip, error)."""
        try:
            console_url, console_user, console_pass = info
            with ConsoleClient(name, console_url, console_user, console_pass) as console:
                # Discover OOB IP (retry for DHCP delay)
                ip_cmd = "ip -4 addr show eth0 | grep -oP 'inet \\K[\\d.]+'"
                ip = None
                for attempt in range(1, 4):
                    rc, out = console.run(ip_cmd, timeout=15)
                    if rc == 0 and out.strip():
                        ip = out.strip().split("\n")[0].strip()
                        break
                    if attempt < 3:
                        time.sleep(5)
                if ip is None:
                    return (name, None, f"Could not get IP after {attempt} attempts: {out}")

                # Write and launch install script
                script = generate_worker_install_script(name)
                script_path = INSTALL_SCRIPT_PATH.format(node=name)
                console.write_file(script_path, script)
                rc, out = console.run(
                    f"sudo chmod +x {script_path} && "
                    f"sudo setsid {script_path} </dev/null &>/dev/null & "
                    f"sleep 1 && echo LAUNCHED",
                    timeout=30,
                )
                if "LAUNCHED" not in out:
                    return (name, ip, f"Launch may have failed: {out[:200]}")
                return (name, ip, None)
        except Exception as exc:
            return (name, None, str(exc))

    results_ip = {}
    errors = {}
    targets = {name: console_info[name] for name in node_vm_map if name in console_info}
    with ThreadPoolExecutor(max_workers=min(len(targets) or 1, MAX_CONSOLE_WORKERS)) as pool:
        futures = {pool.submit(_install_worker, n, info): n for n, info in targets.items()}
        for f in as_completed(futures):
            name, ip, err = f.result()
            if err:
                errors[name] = err
            if ip:
                results_ip[name] = ip

    if errors:
        for name, err in errors.items():
            print(f"    [!] Node install failed for {name}: {err}")

    return results_ip


def wait_for_worker_install(console_info, node_names, timeout=600, poll_interval=20,
                            ui=None):
    log.debug("wait_for_worker_install: nodes=%s timeout=%d", node_names, timeout)
    """Poll nodes for install completion marker. Returns set of names that completed.
    Uses fresh connections each poll to avoid leaving stale shells for Phase 2."""
    def _poll_one(name):
        try:
            wc = ConsoleClient(name, *console_info[name])
            wc.connect(timeout=30)
            try:
                rc, out = wc.run(
                    f"tail -1 {INSTALL_LOG_PATH.format(node=name)} 2>/dev/null || echo PENDING",
                    timeout=15,
                )
                return (name, MARKER_INSTALL_DONE in out)
            finally:
                wc.close()
        except Exception:
            return (name, False)

    completed = set()
    prev_completed = set()
    poll_failures = {n: 0 for n in node_names}
    elapsed = 0
    while elapsed < timeout and len(completed) < len(node_names):
        time.sleep(poll_interval)
        elapsed += poll_interval
        pending_names = [n for n in node_names if n not in completed and n in console_info]
        if pending_names:
            with ThreadPoolExecutor(max_workers=min(len(pending_names), MAX_CONSOLE_WORKERS)) as pool:
                for name, done in pool.map(_poll_one, pending_names):
                    if done:
                        completed.add(name)
                        if ui:
                            ui.mark_done(name)
                    else:
                        poll_failures[name] = poll_failures.get(name, 0) + 1
                        if poll_failures[name] == 5:
                            if ui:
                                ui.mark_error(name, "unreachable")
                            log.debug("[%s] console unreachable after 5 polls", name)
        pending = set(node_names) - completed
        if pending and completed != prev_completed:
            log.debug("[%ds/%ds] installed: %s | pending: %s",
                      elapsed, timeout,
                      ', '.join(completed) or 'none',
                      ', '.join(pending))
            prev_completed = set(completed)
    return completed


def setup_forwarding(console_info, node_vm_map, ui=None):
    """Phase 5: Configure console forwarding, SSH forwarding,
    and create management user on each node."""
    log.debug("Phase 5: setup forwarding for nodes %s", list(node_vm_map.keys()))

    def _setup_node(name):
        """Returns (name, error_or_None)."""
        try:
            kind = node_vm_map[name].get("kind", "nokia_srlinux")
            is_linux = kind == "linux"

            with ConsoleClient(name, *console_info[name],
                               connect_timeout=60, probe_stale=True) as wc:
                script = generate_forwarding_script(name, kind=kind)
                fwd_path = FORWARDING_SCRIPT_PATH.format(node=name)
                wc.write_file(fwd_path, script)
                rc, out = wc.run(
                    f"sudo chmod +x {fwd_path} && "
                    f"sudo {fwd_path}",
                    timeout=120,
                )
                if MARKER_FORWARD_DONE not in out:
                    return (name, f"Script did not complete: {out[-300:]}")

                # Append container auto-attach to ubuntu's .bashrc
                if is_linux:
                    bashrc_snippet = (
                        '\n# Auto-connect to containerlab Linux container for interactive login sessions\n'
                        'if [ -t 0 ] && [ "${SHLVL:-0}" -eq 1 ]; then\n'
                        '    _clab=$(sudo docker ps --format \'{{.Names}}\' '
                        '--filter "name=clab-" 2>/dev/null | head -1)\n'
                        '    if [ -n "$_clab" ]; then\n'
                        '        exec sudo docker exec -it "$_clab" bash\n'
                        '    fi\n'
                        'fi\n'
                    )
                else:
                    bashrc_snippet = (
                        '\n# Auto-connect to SR Linux CLI for interactive login sessions\n'
                        'if [ -t 0 ] && [ "${SHLVL:-0}" -eq 1 ]; then\n'
                        '    _srl=$(sudo docker ps --format \'{{.Names}}\' '
                        '--filter "name=clab-" 2>/dev/null | head -1)\n'
                        '    if [ -n "$_srl" ]; then\n'
                        '        exec sudo docker exec -it "$_srl" sr_cli\n'
                        '    fi\n'
                        'fi\n'
                    )
                wc.write_file("/tmp/clab-bashrc-snippet.sh", bashrc_snippet)
                wc.run("cat /tmp/clab-bashrc-snippet.sh >> /home/ubuntu/.bashrc", timeout=15)

                # Restart getty to activate auto-login — this kills the console
                # session, so use send_cmd (fire-and-forget).
                wc.send_cmd("sudo systemctl restart serial-getty@ttyS0.service")
                log.debug("[%s] Forwarding configured.", name)
                return (name, None)
        except Exception as exc:
            return (name, str(exc))

    errors = {}
    targets = [name for name in node_vm_map
               if name in console_info]
    with ThreadPoolExecutor(max_workers=min(len(targets) or 1, MAX_CONSOLE_WORKERS)) as pool:
        futures = {pool.submit(_setup_node, name): name for name in targets}
        for f in as_completed(futures):
            name, err = f.result()
            if err:
                errors[name] = err
                if ui:
                    ui.mark_error(name, err)
            else:
                if ui:
                    ui.mark_done(name)

    if errors:
        for name, err in errors.items():
            log.debug("Forwarding setup failed for %s: %s", name, err)

    return not errors


# ---------------------------------------------------------------------------
# Deploy — Phase Functions
# ---------------------------------------------------------------------------

def _build_distribution_tree(node_vm_map, vm_ips):
    """Build an exponential doubling tree for peer-to-peer image distribution.

    Tier 0: 1 seed pulls from the internet.
    Tier 1: seed uploads to 1 new node (2 nodes have the image).
    Tier 2: those 2 each upload to 1 new node (4 total).
    Tier 3: those 4 each upload to 4 new nodes (8 total).
    ...each tier doubles the number of nodes that have the image.

    Returns {image: [ [tier_0_entries], [tier_1_entries], ... ]}
    where each entry is {"name": str, "ip": str, "parent_ip": str|None, "has_children": bool}.
    Also returns {image: seed_ip} for fallback purposes.
    """
    # Group nodes by image
    by_image = defaultdict(list)
    for name, info in node_vm_map.items():
        if name in vm_ips:
            by_image[info["image"]].append(name)

    trees = {}
    seed_ips = {}
    for image, names in by_image.items():
        tiers = []
        # Tier 0: pick first node as seed
        seed = names[0]
        tiers.append([{
            "name": seed,
            "ip": vm_ips[seed],
            "parent_ip": None,
            "has_children": False,
        }])
        seed_ips[image] = vm_ips[seed]

        remaining = names[1:]
        # All nodes that have the image so far (available as parents)
        all_ready = [tiers[0][0]]

        while remaining:
            current_tier = []
            # Each ready node serves exactly one new node
            for parent in all_ready:
                if not remaining:
                    break
                child_name = remaining.pop(0)
                parent["has_children"] = True
                current_tier.append({
                    "name": child_name,
                    "ip": vm_ips[child_name],
                    "parent_ip": parent["ip"],
                    "has_children": False,
                })
            tiers.append(current_tier)
            # All nodes (including new ones) are now available as parents
            all_ready = [e for tier in tiers for e in tier]

        trees[image] = tiers

    return trees, seed_ips


def _wait_for_seed_dist(console_info, seed_name, timeout=1200, poll_interval=20,
                        ui=None, conn_semaphore=None):
    """Poll seed node for distribution script completion.
    Returns True if seed is ready (image saved + serving), False on failure/timeout."""
    t0_seed = time.time()
    log.debug("_wait_for_seed_dist: node=%s timeout=%d", seed_name, timeout)
    log_path = DIST_LOG_PATH.format(node=seed_name)
    elapsed = 0
    while elapsed < timeout:
        if _cancel.is_set():
            return False
        time.sleep(poll_interval)
        elapsed += poll_interval
        try:
            if conn_semaphore:
                conn_semaphore.acquire()
            try:
                wc = ConsoleClient(seed_name, *console_info[seed_name])
                wc.connect(timeout=30)
                try:
                    rc, out = wc.run(
                        f"tail -1 {log_path} 2>/dev/null || echo PENDING",
                        timeout=15,
                    )
                finally:
                    wc.close()
            finally:
                if conn_semaphore:
                    conn_semaphore.release()
            if MARKER_DIST_READY in out:
                log.debug("[%s] seed ready, %.1fs", seed_name, time.time() - t0_seed)
                return True
            if MARKER_DIST_FAILED in out:
                log.debug("[%s] seed FAILED, %.1fs", seed_name, time.time() - t0_seed)
                return False
        except Exception:
            pass
    log.debug("[%s] seed TIMEOUT after %ds", seed_name, timeout)
    return False


def _cleanup_tar_on_nodes(node_names, console_info, conn_semaphore):
    """Remove stale tar files on failed nodes so retries start clean."""
    def _cleanup(name):
        conn_semaphore.acquire()
        try:
            with ConsoleClient(name, *console_info[name],
                               connect_timeout=60) as wc:
                wc.run(f"rm -f {IMAGE_TAR_PATH}", timeout=15)
        except Exception:
            pass
        finally:
            conn_semaphore.release()

    with ThreadPoolExecutor(max_workers=min(len(node_names) or 1, MAX_CONSOLE_WORKERS)) as pool:
        list(pool.map(_cleanup, node_names))


def _distribute_single_image(image, tiers, seed_ip, console_info,
                             conn_semaphore, ui=None):
    """Distribute a single container image across its tier tree.

    Tiers are processed sequentially (tier N depends on tier N-1).
    Returns a set of node names that failed distribution for this image.
    """
    image_failed = set()

    for tier_idx, tier in enumerate(tiers):
        if _cancel.is_set():
            return image_failed
        tier_label = f"tier-{tier_idx}"
        names = [e["name"] for e in tier]
        log.debug("[%s][%s] %s", image, tier_label, ', '.join(names))

        def _distribute_node(entry, _tier_idx=tier_idx, _image=image,
                             _seed_ip=seed_ip):
            """Returns (name, error_or_None)."""
            name = entry["name"]
            if _cancel.is_set():
                return (name, "cancelled")
            conn_semaphore.acquire()
            try:
                with ConsoleClient(name, *console_info[name],
                                   connect_timeout=120) as wc:
                    if _tier_idx == 0:
                        # Seed: launch background script (pull + save + serve)
                        script = generate_seed_dist_script(
                            name, _image, entry["has_children"])
                        script_path = DIST_SCRIPT_PATH.format(node=name)
                        wc.write_file(script_path, script)
                        rc, out = wc.run(
                            f"sudo chmod +x {script_path} && "
                            f"sudo setsid {script_path} </dev/null &>/dev/null & "
                            f"sleep 1 && echo LAUNCHED",
                            timeout=30,
                        )
                        if "LAUNCHED" not in out:
                            return (name, f"Seed script launch failed: {out[:200]}")
                        return (name, "__POLL__")
                    else:
                        # Non-seed: download from parent, load, optionally serve
                        # Clean up any stale tar from a previous failed attempt
                        wc.run(f"rm -f {IMAGE_TAR_PATH}", timeout=15)
                        parent_ip = entry["parent_ip"]
                        rc, out = wc.run(
                            f"curl -sS --retry 3 --retry-delay 5 -o {IMAGE_TAR_PATH} "
                            f"http://{parent_ip}:{DIST_HTTP_PORT}/{Path(IMAGE_TAR_PATH).name}",
                            timeout=CMD_TIMEOUT,
                        )
                        if rc != 0:
                            # Fallback to seed node
                            if parent_ip != _seed_ip:
                                log.debug("[%s] Parent failed, falling back to seed", name)
                                rc, out = wc.run(
                                    f"curl -sS --retry 3 --retry-delay 5 -o {IMAGE_TAR_PATH} "
                                    f"http://{_seed_ip}:{DIST_HTTP_PORT}/{Path(IMAGE_TAR_PATH).name}",
                                    timeout=CMD_TIMEOUT,
                                )
                            if rc != 0:
                                return (name, f"download failed: {out[-300:]}")
                        rc, out = wc.run(
                            f"gunzip -c {IMAGE_TAR_PATH} | sudo docker load",
                            timeout=DOCKER_LOAD_TIMEOUT,
                        )
                        if rc != 0:
                            return (name, f"docker load failed: {out[-300:]}")
                        if entry["has_children"]:
                            wc.run(
                                f"cd /tmp && sudo setsid python3 -m http.server {DIST_HTTP_PORT} "
                                f"</dev/null &>/dev/null & "
                                f"sleep 1 && echo SERVING",
                                timeout=15,
                            )
                    return (name, None)
            except Exception as exc:
                return (name, str(exc))
            finally:
                conn_semaphore.release()

        errors = {}
        poll_seeds = []
        # Mark nodes in this tier as started so their timer begins now
        if ui:
            for entry in tier:
                ui.mark_start(entry["name"])
        with ThreadPoolExecutor(max_workers=min(len(tier) or 1, MAX_CONSOLE_WORKERS)) as pool:
            futures = {pool.submit(_distribute_node, entry): entry["name"]
                       for entry in tier}
            for f in as_completed(futures):
                name, err = f.result()
                if err == "__POLL__":
                    poll_seeds.append(name)
                elif err:
                    errors[name] = err
                    if ui:
                        ui.mark_error(name, err)
                else:
                    if ui:
                        ui.mark_done(name)

        # Tier-0 seed: poll for background script completion
        if poll_seeds:
            log.debug("Seed launched: %s", ', '.join(poll_seeds))
            for seed_name in poll_seeds:
                if _cancel.is_set():
                    image_failed.add(seed_name)
                    continue
                ready = _wait_for_seed_dist(console_info, seed_name,
                                            ui=ui, conn_semaphore=conn_semaphore)
                if ready:
                    log.debug("[%s] Seed ready", seed_name)
                    if ui:
                        ui.mark_done(seed_name)
                else:
                    log.debug("[%s] Seed distribution failed or timed out", seed_name)
                    if ui:
                        ui.mark_error(seed_name, "seed dist failed")
                    image_failed.add(seed_name)
                    # Skip remaining tiers for this image — no seed to serve from
                    errors[seed_name] = "seed dist failed"
            if errors:
                break  # Skip downstream tiers
            continue  # Tier-0 done, proceed to tier-1

        # Retry failed nodes in this tier once
        if errors:
            log.debug("Tier %d: %d failed, retrying ...", tier_idx, len(errors))
            if _cancel.is_set():
                image_failed.update(errors.keys())
                continue
            time.sleep(10)
            # Clean up stale tar files on failed nodes before retry
            _cleanup_tar_on_nodes(
                [e["name"] for e in tier if e["name"] in errors],
                console_info, conn_semaphore)
            retry_entries = [e for e in tier if e["name"] in errors]
            errors = {}
            with ThreadPoolExecutor(max_workers=min(len(retry_entries) or 1, MAX_CONSOLE_WORKERS)) as pool:
                futures = {pool.submit(_distribute_node, entry): entry["name"]
                           for entry in retry_entries}
                for f in as_completed(futures):
                    name, err = f.result()
                    if err:
                        errors[name] = err
            if errors:
                log.debug("Retry: %d still failed", len(errors))
                # If there are more tiers, push failed nodes to next tier
                # so they retry from seed. Otherwise mark as final failure.
                if tier_idx < len(tiers) - 1:
                    failed_entries = [
                        {"name": e["name"], "ip": e["ip"],
                         "parent_ip": seed_ip, "has_children": False}
                        for e in tier if e["name"] in errors
                    ]
                    tiers[tier_idx + 1].extend(failed_entries)
                    log.debug("Tier %d: %d nodes deferred to tier %d for retry from seed",
                              tier_idx, len(failed_entries), tier_idx + 1)
                else:
                    log.debug("Tier %d: final tier, %d nodes permanently failed",
                              tier_idx, len(errors))
                    image_failed.update(errors.keys())
            else:
                log.debug("Retry: all recovered")

    return image_failed


def _phase_2_distribute(console_info, node_vm_map, vm_ips, ui=None):
    """Phase 2: Distribute container images via exponential doubling tree.

    Different images are distributed in parallel (each in its own thread).
    Tiers within a single image are processed sequentially.

    Returns set of node names that failed image distribution (skipped in Phase 3).
    """
    log.debug("Phase 2: distribute images for nodes %s", list(node_vm_map.keys()))
    trees, seed_ips = _build_distribution_tree(node_vm_map, vm_ips)
    if not trees:
        return set()

    total_nodes = sum(sum(len(t) for t in tiers) for tiers in trees.values())
    log.debug("Phase 2: %d nodes across %d image(s)", total_nodes, len(trees))

    conn_semaphore = threading.Semaphore(MAX_CONSOLE_WORKERS)
    all_failed = set()

    with ThreadPoolExecutor(max_workers=len(trees)) as pool:
        futures = {
            pool.submit(_distribute_single_image, image, tiers, seed_ips[image],
                        console_info, conn_semaphore, ui): image
            for image, tiers in trees.items()
        }
        for fut in as_completed(futures):
            image = futures[fut]
            try:
                image_failed = fut.result()
                all_failed.update(image_failed)
            except Exception as exc:
                log.debug("Image %s distribution raised: %s", image, exc)
                for tier in trees[image]:
                    for entry in tier:
                        all_failed.add(entry["name"])

    return all_failed


def _phase_1_install(console_info, node_vm_map, ui=None):
    """Phase 1: Install Docker+clab on all nodes.
    Returns (vm_ips, installed) or raises on failure."""
    log.debug("Phase 1: install on nodes %s", list(node_vm_map.keys()))

    # Discover node IPs + launch install scripts on all nodes
    vm_ips = setup_workers_install(console_info, node_vm_map)
    missing = set(node_vm_map.keys()) - set(vm_ips.keys())
    if missing:
        raise RuntimeError(f"Could not discover IPs for: {', '.join(missing)}")

    # Wait for node install scripts to finish (Docker + clab ready)
    installed = wait_for_worker_install(console_info, list(vm_ips.keys()),
                                        ui=ui)
    not_installed = set(vm_ips.keys()) - installed
    if not_installed:
        raise RuntimeError(f"Install did not complete for: {', '.join(not_installed)}")

    return vm_ips, installed


def _phase_3_deploy(console_info, node_vm_map, installed, ui=None):
    """Phase 3: Send deploy scripts to all installed nodes.
    Returns True on success, raises RuntimeError on failure."""
    log.debug("Phase 3: deploy on nodes %s", list(installed))
    def _send_deploy(name, info):
        """Returns (name, error_or_None)."""
        if _cancel.is_set():
            return (name, "cancelled")
        try:
            with ConsoleClient(name, *console_info[name],
                               connect_timeout=120,
                               probe_stale=True) as wc:
                # Write startup config as a separate file (avoids heredoc issues)
                cfg_path = info.get("startup_config")
                if cfg_path:
                    config_content = cfg_path.read_text(encoding="utf-8")
                    wc.write_file(STARTUP_CFG_PATH.format(node=name), config_content)

                script = generate_worker_deploy_script(name, info)
                wc.write_file(WORKER_SCRIPT_PATH.format(node=name), script)
                rc, out = wc.run(
                    f"sudo chmod +x {WORKER_SCRIPT_PATH.format(node=name)} && "
                    f"sudo setsid {WORKER_SCRIPT_PATH.format(node=name)} </dev/null &>/dev/null & "
                    f"sleep 1 && echo LAUNCHED",
                    timeout=30,
                )
                if "LAUNCHED" in out:
                    return (name, None)
                else:
                    return (name, f"Launch may have failed: {out[:200]}")
        except Exception as exc:
            return (name, str(exc))

    def _run_deploy_batch(items):
        errors = {}
        with ThreadPoolExecutor(max_workers=min(len(items) or 1, MAX_CONSOLE_WORKERS)) as pool:
            futures = {pool.submit(_send_deploy, n, info): n for n, info in items}
            for f in as_completed(futures):
                name, err = f.result()
                if err:
                    errors[name] = err
        return errors

    deploy_errors = _run_deploy_batch(
        [(n, info) for n, info in node_vm_map.items() if n in installed])

    # Retry failed nodes up to 3 more times with increasing backoff
    for retry_num, backoff in enumerate([15, 30, 60], start=1):
        if not deploy_errors:
            break
        for name, err in deploy_errors.items():
            log.debug("Deploy script send failed for %s: %s", name, err)
        retry_names = list(deploy_errors.keys())
        log.debug("Retry %d: %s (waiting %ds)", retry_num, ', '.join(retry_names), backoff)
        time.sleep(backoff)
        deploy_errors = _run_deploy_batch(
            [(n, node_vm_map[n]) for n in retry_names])

    if deploy_errors:
        for name, err in deploy_errors.items():
            log.debug("Deploy send failed for %s after retries: %s", name, err)
            if ui:
                ui.mark_error(name, err)
        raise RuntimeError(
            f"Deploy script send failed for {len(deploy_errors)} node(s)")

    return True


def _phase_4_monitor(console_info, installed, poll_timeout=900, poll_interval=30,
                     ui=None):
    """Phase 4: Poll nodes for deploy completion marker.
    Uses fresh connections each poll to avoid leaving stale shells for Phase 5.
    Returns (done_set, pending_list, timed_out)."""
    log.debug("Phase 4: monitor deployment of %d nodes", len(installed))

    def _poll_deploy(name):
        try:
            wc = ConsoleClient(name, *console_info[name])
            wc.connect(timeout=30)
            try:
                rc, out = wc.run(
                    f"tail -1 {WORKER_LOG_PATH.format(node=name)} 2>/dev/null || echo PENDING",
                    timeout=15,
                )
                return (name, "SUCCESS" if MARKER_DEPLOY_DONE in out else "PENDING")
            finally:
                wc.close()
        except Exception:
            return (name, "PENDING")

    def _fetch_log_tail(name):
        try:
            wc = ConsoleClient(name, *console_info[name])
            wc.connect(timeout=30)
            try:
                rc, out = wc.run(
                    f"tail -20 {WORKER_LOG_PATH.format(node=name)} 2>/dev/null",
                    timeout=15,
                )
                return (name, out[-500:], None)
            finally:
                wc.close()
        except Exception as exc:
            return (name, None, str(exc))

    elapsed = 0
    done = set()
    prev_done = set()
    pending = list(installed)
    while elapsed < poll_timeout:
        time.sleep(poll_interval)
        elapsed += poll_interval

        worker_status = {name: "SUCCESS" for name in done}
        for name in installed:
            if name in done or name not in console_info:
                if name not in console_info:
                    worker_status[name] = "NO_CONSOLE"
                continue

        pending_names = [n for n in installed if n not in done and n in console_info]
        if pending_names:
            with ThreadPoolExecutor(max_workers=min(len(pending_names), MAX_CONSOLE_WORKERS)) as pool:
                for name, status in pool.map(_poll_deploy, pending_names):
                    worker_status[name] = status

        # Detect newly completed nodes and mark them in the UI
        new_done = {n for n, s in worker_status.items() if s == "SUCCESS"}
        for name in new_done - done:
            if ui:
                ui.mark_done(name)
        done = new_done
        pending = [n for n, s in worker_status.items() if s not in ("SUCCESS", "NO_CONSOLE")]

        if not pending:
            log.debug("All %d nodes deployed successfully", len(done))
            break
        elif done != prev_done:
            log.debug("[%ds/%ds] done: %s | pending: %s",
                      elapsed, poll_timeout,
                      ', '.join(done) or 'none', ', '.join(pending))
            prev_done = set(done)
    else:
        if ui:
            ui.stop()
        print(f"\n[-] Deployment timed out after {poll_timeout}s. "
              f"Still pending: {', '.join(pending)}", file=sys.stderr)
        log_targets = [n for n in pending if n in console_info]
        if log_targets:
            with ThreadPoolExecutor(max_workers=min(len(log_targets), MAX_CONSOLE_WORKERS)) as pool:
                for name, log_tail, err in pool.map(_fetch_log_tail, log_targets):
                    if log_tail:
                        print(f"\n    --- {name} log tail ---")
                        print(f"    {log_tail}")
                    elif err:
                        print(f"    Could not get log for {name}: {err}")
        return done, pending, True

    return done, pending, False


# ---------------------------------------------------------------------------
# Deploy — Orchestrator
# ---------------------------------------------------------------------------

_active_deploy = {"api": None, "sim": None}


def _cleanup_deploy(api):
    """Destroy the in-progress simulation after a keyboard interrupt."""
    sim = _active_deploy.get("sim")
    if not sim:
        return
    sim_id = sim.id
    print(f"\n[+] Cleaning up simulation {sim_id} ...")
    try:
        sim.delete()
        print(f"[+] Simulation {sim_id} deleted.")
    except Exception as exc:
        print(f"[!] Cleanup failed: {exc}", file=sys.stderr)
        print(f"    Delete manually: {AIR_WEB_URL}/simulations/{sim_id}")


def deploy_lab(api: AirApi, clab_path: Path, title: str,
               organization: str = None, start: bool = True) -> str:

    log.debug("deploy_lab(clab_path=%s, title=%s, org=%s, start=%s)",
              clab_path, title, organization, start)
    ui = DeployUI()

    # --- Parsing ---
    print(f"[+] Parsing containerlab topology: {clab_path}")
    clab_data = parse_clab_yaml(clab_path)
    print(f"    Lab name : {clab_data['name']}")
    print(f"    Nodes    : {len(clab_data['nodes'])} ({', '.join(clab_data['nodes'].keys())})")
    print(f"    Links    : {len(clab_data['links'])}")

    air_content, node_vm_map = build_air_topology(clab_data)
    title = title or clab_data["name"]
    node_names = list(node_vm_map.keys())

    # --- Initiating Simulation ---
    print("\n[+] Initiating Simulation on Nvidia Air")
    kwargs = {}
    if organization:
        kwargs["organization"] = organization
    sim = api.simulations.create_from(title, "JSON", air_content, **kwargs)
    _active_deploy["sim"] = sim
    sim_id = sim.id
    log.debug("Simulation created: id=%s title=%s", sim_id, title)
    sim_url = f"{AIR_WEB_URL}/simulations/{sim_id}"
    print(f"    ID    : {sim_id}")

    if not start:
        print(f"    State : created (not started)")
        print(f"    URL   : {sim_url}")
        return str(sim_id)

    # Start and wait for LOADED — spinner on state line
    load_deadline = time.time() + VM_BOOT_TIMEOUT
    while True:
        try:
            sim.load()
            break
        except Exception as load_exc:
            if "out of capacity" in str(load_exc).lower():
                if time.time() > load_deadline:
                    raise
                print(f"    Air out of capacity, retrying in 60s ...")
                time.sleep(60)
                continue
            raise

    is_tty = sys.stdout.isatty()
    spin_idx = 0
    deadline = time.time() + VM_BOOT_TIMEOUT
    while time.time() < deadline:
        sim.refresh()
        state = sim.state
        if is_tty:
            char = DeployUI.SPINNER_CHARS[spin_idx % 4]
            sys.stdout.write(f"\r\033[K    State : {state} {char}")
            sys.stdout.flush()
            spin_idx += 1
        else:
            print(f"    State : {state}")
        if state == "LOADED":
            if is_tty:
                sys.stdout.write(f"\r\033[K    State : {state}\n")
                sys.stdout.flush()
            break
        if state == "ERROR":
            if is_tty:
                sys.stdout.write(f"\r\033[K    State : {state}\n")
                sys.stdout.flush()
            print(f"[-] Simulation entered ERROR state!", file=sys.stderr)
            print(f"    URL   : {sim_url}")
            return str(sim_id)
        time.sleep(10)
    else:
        if is_tty:
            sys.stdout.write("\n")
        print("[-] Timed out waiting for LOADED state.", file=sys.stderr)
        print(f"    URL   : {sim_url}")
        return str(sim_id)

    # --- Accessing the nodes ---
    all_names = set(node_vm_map.keys())
    print(f"\n[+] Accessing the nodes ...")
    console_info = get_console_info(api, sim, all_names)

    t_total = time.time()

    # --- Installing Docker and Containerlab ---
    t_phase = time.time()
    print(f"\n[+] Installing Docker and Containerlab on {len(node_vm_map)} nodes ...")
    ui.begin(node_names)
    try:
        vm_ips, installed = _phase_1_install(console_info, node_vm_map, ui=ui)
    except RuntimeError as exc:
        ui.stop()
        print(f"[-] {exc}. Aborting.", file=sys.stderr)
        print(f"    URL: {sim_url}")
        return str(sim_id)
    ui.stop()
    print(f"    Completed in {time.time() - t_phase:.0f}s")

    # --- Downloading Images ---
    t_phase = time.time()
    print(f"\n[+] Downloading Images on {len(node_vm_map)} nodes ...")
    ui.begin(node_names, start_all=False)
    dist_failed = _phase_2_distribute(console_info, node_vm_map, vm_ips, ui=ui)
    ui.stop()
    if dist_failed:
        log.debug("%d node(s) failed image distribution: %s",
                  len(dist_failed), ', '.join(dist_failed))
        print(f"    Completed in {time.time() - t_phase:.0f}s")
        print(f"[-] Image distribution failed for {len(dist_failed)} node(s): "
              f"{', '.join(sorted(dist_failed))}", file=sys.stderr)
        print(f"    URL: {sim_url}")
        return str(sim_id)
    print(f"    Completed in {time.time() - t_phase:.0f}s")

    # Brief pause to let console connections settle
    time.sleep(20)

    # --- Deploying ContainerLabs (Phase 3 + Phase 4 merged) ---
    t_phase = time.time()
    print(f"\n[+] Deploying ContainerLabs on {len(installed)} nodes ...")
    ui.begin(list(installed))
    try:
        _phase_3_deploy(console_info, node_vm_map, installed, ui=ui)
    except RuntimeError as exc:
        ui.stop()
        print(f"[-] {exc}. Aborting.", file=sys.stderr)
        print(f"    URL: {sim_url}")
        return str(sim_id)

    done, pending, timed_out = _phase_4_monitor(console_info, installed, ui=ui)
    ui.stop()
    if timed_out:
        print(f"    URL: {sim_url}")
        return str(sim_id)
    print(f"    Completed in {time.time() - t_phase:.0f}s")

    # --- Post Deployment Tasks ---
    t_phase = time.time()
    if node_vm_map:
        print(f"\n[+] Post Deployment Tasks (Console and SSH forwarding) "
              f"on {len(node_vm_map)} nodes ...")
        ui.begin(node_names)
        setup_forwarding(console_info, node_vm_map, ui=ui)
        ui.stop()
        srl_nodes = [n for n, v in node_vm_map.items() if v.get("kind") != "linux"]
        linux_nodes = [n for n, v in node_vm_map.items() if v.get("kind") == "linux"]
        if srl_nodes:
            print(f"    SRL nodes:   console -> sr_cli, SSH 22 -> SRL mgmt (admin/NokiaSrl1!)")
        if linux_nodes:
            print(f"    Linux nodes: console -> container bash, SSH 22 -> container")
    print(f"    Completed in {time.time() - t_phase:.0f}s")

    # --- Deployment Complete Summary ---
    print(f"\n[+] Deployment Complete")
    print(f"    Nodes : {len(node_vm_map)} ({', '.join(node_names)})")
    print(f"    Time  : {time.time() - t_total:.0f}s")
    print(f"    ID    : {sim_id}")
    print(f"    URL   : {sim_url}")
    _active_deploy["sim"] = None
    return str(sim_id)


# ---------------------------------------------------------------------------
# Shared Helpers
# ---------------------------------------------------------------------------

def _lookup_simulations(api, title, sim_id=None):
    """Look up simulations by ID or title. Returns list (may be empty)."""
    if sim_id:
        return [api.simulations.get(sim_id)]
    print(f"[+] Looking up simulations with title '{title}' ...")
    sims = [s for s in api.simulations.list() if s.title == title]
    if not sims:
        print(f"[-] No simulation found with title '{title}'.")
    return sims


# ---------------------------------------------------------------------------
# Destroy
# ---------------------------------------------------------------------------

def destroy(api: AirApi, title: str, sim_id: str = None) -> None:
    log.debug("destroy(title=%s, sim_id=%s)", title, sim_id)
    sims = _lookup_simulations(api, title, sim_id)
    if not sims:
        return

    print(f"[+] Found {len(sims)} simulation(s) to delete.")
    for sim in sims:
        sid = sim.id
        state = sim.state
        print(f"\n[+] Simulation: {sid}  (state: {state})")

        if state in ("LOADING", "STORING"):
            print(f"    Waiting for transitional state '{state}' ...")
            deadline = time.time() + 300
            while time.time() < deadline:
                sim.refresh()
                state = sim.state
                if state not in ("LOADING", "STORING"):
                    break
                time.sleep(10)

        print(f"[+] Deleting simulation {sid} ...")
        sim.delete()
        print(f"[+] Simulation {sid} deleted.")


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def status(api: AirApi, title: str, sim_id: str = None) -> None:
    sims = _lookup_simulations(api, title, sim_id)
    if not sims:
        return

    for sim in sims:
        sid = sim.id
        print(f"\n  Simulation")
        print(f"  ---------")
        print(f"  ID      : {sid}")
        print(f"  Title   : {sim.title}")
        print(f"  State   : {sim.state}")
        print(f"  URL     : {AIR_WEB_URL}/simulations/{sid}")

        nodes = list(api.nodes.list(simulation=sim))
        if nodes:
            print(f"\n  Nodes ({len(nodes)})")
            print(f"  -----")
            for n in nodes:
                print(f"  - {n.name:16s}  state={getattr(n, 'state', '?')}")

        interfaces = list(api.interfaces.list(simulation=sim))
        if interfaces:
            by_node = {}
            for iface in interfaces:
                nid = _get_node_pk(iface.node)
                by_node.setdefault(nid, []).append(iface)
            node_names = {str(n.id): n.name for n in nodes}
            print(f"\n  Interfaces ({len(interfaces)})")
            print(f"  ----------")
            for nid, ifaces in by_node.items():
                name = node_names.get(nid, nid)
                iface_names = sorted(getattr(i, "name", "?") for i in ifaces)
                print(f"  - {name:16s}: {', '.join(iface_names)}")

        links = list(api.links.list(simulation=sim))
        if links:
            iface_map = {str(i.id): i for i in interfaces}
            node_names_map = {str(n.id): n.name for n in nodes}
            print(f"\n  Links ({len(links)})")
            print(f"  -----")
            for lnk in links:
                si = getattr(lnk, "simulation_interfaces", [])
                if len(si) == 2:
                    a = iface_map.get(str(getattr(si[0], 'id', si[0])))
                    b = iface_map.get(str(getattr(si[1], 'id', si[1])))
                    if a and b:
                        an = node_names_map.get(_get_node_pk(a.node), "?")
                        bn = node_names_map.get(_get_node_pk(b.node), "?")
                        print(f"  - {an}:{getattr(a, 'name', '?')} <---> "
                              f"{bn}:{getattr(b, 'name', '?')}")

        # Show SSH services
        try:
            services = list(api.services.list(simulation=sim))
            if services:
                print(f"\n  SSH Services ({len(services)})")
                print(f"  ------------")
                for svc in services:
                    print(f"  - {svc.name:16s}  {svc.host}:{svc.src_port} -> :{svc.dest_port}")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _resolve_title(args):
    """Resolve (title, sim_id) from CLI args, topology file, or glob fallback."""
    if args.title or args.sim_id:
        return args.title, args.sim_id
    if args.topology:
        clab_data = parse_clab_yaml(Path(args.topology))
        return clab_data["name"], None
    candidates = sorted(WORK_DIR.glob("*.clab.yml"))
    if candidates:
        clab_data = parse_clab_yaml(candidates[0])
        return clab_data["name"], None
    print("[-] Specify --title or --sim-id.", file=sys.stderr)
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Deploy containerlab topologies on Nvidia Air",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 clabonair.py deploy -t 2node.clab.yml
  python3 clabonair.py deploy -t 2node.clab.yml --title my-lab
  python3 clabonair.py destroy --title my-lab
  python3 clabonair.py destroy --sim-id <uuid>
  python3 clabonair.py status --title my-lab
        """,
    )
    parser.add_argument("action", choices=["deploy", "destroy", "status"])
    parser.add_argument("--title", help="Simulation title")
    parser.add_argument("--sim-id", dest="sim_id", help="Simulation UUID")
    parser.add_argument("-t", dest="topology", help="Path to containerlab YAML file")
    parser.add_argument("--no-start", dest="start", action="store_false", default=True)
    parser.add_argument("--vars", default=str(VARS_FILE),
                        help=f"Path to env vars file (default: {VARS_FILE})")
    parser.add_argument("--debug", action="store_true",
                        help="Enable detailed debug logging to clabonair-debug.log")

    args = parser.parse_args()

    if args.debug:
        setup_debug_logging()
    cfg = load_env(Path(args.vars))

    try:
        api = connect(cfg)

        if args.action == "deploy":
            if not args.topology:
                candidates = sorted(WORK_DIR.glob("*.clab.yml"))
                if len(candidates) == 1:
                    topo_path = candidates[0]
                elif len(candidates) > 1:
                    print("[-] Multiple .clab.yml files found. Use -t to specify.",
                          file=sys.stderr)
                    sys.exit(1)
                else:
                    print("[-] No .clab.yml file found. Use -t to specify.",
                          file=sys.stderr)
                    sys.exit(1)
            else:
                topo_path = Path(args.topology)
                if not topo_path.exists():
                    print(f"[-] File not found: {topo_path}", file=sys.stderr)
                    sys.exit(1)

            org_value = cfg.get("ORGANIZATION") or cfg.get("ORGANIZATION_ID")
            org_id = resolve_organization(api, org_value) if org_value else None
            try:
                sim_id = deploy_lab(api, topo_path, args.title,
                                    organization=org_id, start=args.start)
            except KeyboardInterrupt:
                _cancel.set()  # Signal background threads to stop
                print("\n[!] Interrupted.")
                _cleanup_deploy(api)
                sys.exit(130)
            print(f"\n[+] Done. Simulation ID: {sim_id}")
            print(f"          URL: {AIR_WEB_URL}/simulations/{sim_id}")

        elif args.action == "destroy":
            title, sim_id = _resolve_title(args)
            destroy(api, title or "", sim_id=sim_id)
            print("\n[+] Done.")

        elif args.action == "status":
            title, sim_id = _resolve_title(args)
            status(api, title or "", sim_id=sim_id)

    except KeyboardInterrupt:
        print("\n[!] Interrupted.")
        sys.exit(130)
    except Exception as exc:
        print(f"\n[-] Error: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
