#!/usr/bin/env python3
"""
Interactive console for Nvidia AIR nodes.

Usage:
  python3 airconsole.py              # list nodes from all LOADED simulations
  python3 airconsole.py --sim-id ID  # list nodes from a specific simulation
"""

import sys
import os
import tty
import termios
import socket
import base64
import select
import time
import threading
from pathlib import Path

import logging
import paramiko
import websockets.sync.client as ws_sync

# Suppress noisy websockets keepalive errors on connection teardown
logging.getLogger("websockets").setLevel(logging.CRITICAL)
logging.getLogger("paramiko").setLevel(logging.CRITICAL)

# Re-use helpers from clabonair.py
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))
from clabonair import load_env, connect, VARS_FILE, _ws_bridge


IGNORED_NODES = {"oob-mgmt-server", "oob-mgmt-switch", "image-registry"}


def list_nodes(api, sim_id=None, title=None):
    """Return a list of dicts with node console details."""
    if sim_id:
        sims = [api.simulations.get(sim_id)]
    else:
        sims = [s for s in api.simulations.list()
                if s.state == "LOADED"
                and (not title or title.lower() in s.title.lower())]

    entries = []
    for sim in sims:
        nodes = list(api.nodes.list(simulation=sim))
        for n in nodes:
            if n.name in IGNORED_NODES:
                continue
            console_url = getattr(n, "console_url", None)
            console_user = getattr(n, "console_username", None)
            console_pass = getattr(n, "console_password", None)
            if console_url and console_user:
                entries.append({
                    "sim_title": sim.title,
                    "sim_id": str(sim.id),
                    "name": n.name,
                    "console_url": console_url,
                    "console_username": console_user,
                    "console_password": console_pass,
                })
    return entries


SRL_DEFAULT_USER = "admin"
SRL_DEFAULT_PASS = "NokiaSrl1!"


def _is_srlinux(node):
    """Heuristic: treat a node as SRLinux if its name contains 'srl'."""
    return "srl" in node["name"].lower()


def _read_until_console(channel, marker, timeout=10):
    """Read from channel until marker string is found or timeout."""
    buf = b""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            data = channel.recv(4096)
            if not data:
                break
            buf += data
            if marker.encode() in buf:
                return buf
        except socket.timeout:
            continue
    raise TimeoutError(f"Timed out waiting for '{marker}'")


def interactive_console(node):
    """SSH over WebSocket, then bridge the shell channel to the terminal."""
    console_url = node["console_url"]
    console_user = node["console_username"]
    console_pass = node["console_password"]

    print(f"[+] Connecting to console ({node['name']}) ...")

    # Set up WebSocket <-> socket bridge
    param_sock, bridge_sock = socket.socketpair()
    threading.Thread(
        target=_ws_bridge, args=(console_url, bridge_sock), daemon=True
    ).start()
    time.sleep(1)

    # SSH over the bridged socket
    transport = paramiko.Transport(param_sock)
    transport.start_client()
    transport.auth_password(console_user, console_pass)

    # Get terminal size
    try:
        cols, rows = os.get_terminal_size()
    except OSError:
        cols, rows = 120, 40

    channel = transport.open_session()
    channel.get_pty(term=os.environ.get("TERM", "xterm-256color"), width=cols, height=rows)
    channel.invoke_shell()

    # For SRLinux nodes, auto-login with default credentials.
    # Send an initial Enter to clear any stale prompt, wait for the
    # login prompt, then send username/password with small delays
    # (mirrors the approach in air.py that handles garbled prompts).
    if _is_srlinux(node):
        print(f"[+] SRLinux node detected — attempting default login (admin) ...")
        channel.settimeout(5)
        try:
            channel.send("\n")
            _read_until_console(channel, "login:")
            time.sleep(0.5)
            channel.send(SRL_DEFAULT_USER + "\n")
            _read_until_console(channel, "Password:")
            time.sleep(0.3)
            channel.send(SRL_DEFAULT_PASS + "\n")
            print(f"[+] Default credentials sent.")
        except (socket.timeout, TimeoutError):
            print(f"[!] Could not detect login prompt — enter credentials manually.")
        channel.settimeout(None)

    print(f"[+] Connected. Press Ctrl+] to disconnect.\n")

    old_settings = termios.tcgetattr(sys.stdin)
    try:
        tty.setraw(sys.stdin.fileno())

        while True:
            r, _, _ = select.select([channel, sys.stdin], [], [], 0.1)

            if channel in r:
                try:
                    data = channel.recv(4096)
                    if not data:
                        break
                    os.write(sys.stdout.fileno(), data)
                except socket.timeout:
                    pass

            if sys.stdin in r:
                data = os.read(sys.stdin.fileno(), 1024)
                if not data:
                    break
                # Ctrl+] (0x1d) to disconnect
                if b"\x1d" in data:
                    break
                channel.sendall(data)

    finally:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
        channel.close()
        transport.close()
        param_sock.close()
        print("\n[+] Disconnected.")


def pick_one(prompt, items, key):
    """Let the user pick an item by number, partial name prefix, or full name.

    Loops until a valid selection is made. Ctrl+C / Ctrl+D to cancel.

    Args:
        prompt: Input prompt string.
        items: List of dicts.
        key: Dict key whose value is the display name.

    Returns:
        The selected item dict.
    """
    print()
    for i, item in enumerate(items, 1):
        print(f"  {i:>3}  {item[key]}")

    while True:
        print()
        try:
            choice = input(prompt).strip()
        except (EOFError, KeyboardInterrupt):
            print("\n[!] Cancelled.")
            sys.exit(0)

        if not choice:
            continue

        # Try as a number first
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(items):
                return items[idx]
            print(f"[-] Number out of range (1-{len(items)}). Try again.")
            continue
        except ValueError:
            pass

        # Try as a name prefix / full name (case-insensitive)
        query = choice.lower()
        matches = [item for item in items if item[key].lower().startswith(query)]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            print(f"[-] Ambiguous input '{choice}' — matches: {', '.join(m[key] for m in matches)}")
            continue

        print(f"[-] No match for '{choice}'. Try again.")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Interactive console for Nvidia AIR nodes")
    parser.add_argument("--vars", default=str(VARS_FILE), help="Path to env vars file")
    args = parser.parse_args()

    cfg = load_env(Path(args.vars))
    api = connect(cfg)

    # Gather LOADED simulations
    sims = sorted(
        [s for s in api.simulations.list() if s.state == "LOADED"],
        key=lambda s: s.title.lower(),
    )
    if not sims:
        print("[-] No LOADED simulations found.")
        sys.exit(1)

    sim_items = [{"title": s.title, "id": str(s.id), "_sim": s} for s in sims]

    # If only one simulation, auto-select it
    if len(sim_items) == 1:
        selected_sim = sim_items[0]
        print(f"\n[+] Only one simulation: {selected_sim['title']}")
    else:
        print(f"\n  Simulations ({len(sim_items)}):")
        selected_sim = pick_one("  Select simulation: ", sim_items, "title")

    # List nodes for the selected simulation
    entries = list_nodes(api, sim_id=selected_sim["id"])
    if not entries:
        print("[-] No reachable nodes found in this simulation.")
        sys.exit(1)

    entries.sort(key=lambda e: e["name"].lower())

    print(f"\n  Nodes in '{selected_sim['title']}' ({len(entries)}):")
    selected_node = pick_one("  Select node: ", entries, "name")

    print(f"\n[+] Opening console to '{selected_node['name']}' ({selected_sim['title']})")
    interactive_console(selected_node)


if __name__ == "__main__":
    main()
