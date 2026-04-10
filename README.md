# clabonair

Deploy [containerlab](https://containerlab.dev/) topologies on [Nvidia AIR](https://air.nvidia.com/) cloud infrastructure.

clabonair reads a standard containerlab YAML topology file, creates matching Ubuntu VMs on Nvidia AIR with the correct links between them, installs Docker and containerlab inside each VM, distributes container images across the VMs, and deploys single-node containerlab topologies on each one. The result is a fully running multi-node lab accessible through the Nvidia AIR web console.

A companion script, `airconsole.py`, provides interactive terminal access to any node in a running simulation directly from your local machine.

## Prerequisites

- Python 3.8+
- An Nvidia AIR account with an API token (get one from <https://air.nvidia.com/account/api-tokens>)
- The following Python packages:
  - `pyyaml`
  - `paramiko`
  - `websockets`
  - `air-sdk`

Install dependencies:

```bash
pip install pyyaml paramiko websockets air-sdk
```

## Setup

On first run, clabonair creates a credentials file called `air.vars` in your working directory. Fill it in with your Nvidia AIR credentials:

```
EMAIL=you@example.com
API_TOKEN=your-api-token-here

# Optional: target a specific organization (name or UUID)
ORGANIZATION=my-org
```

## Usage

### Deploying a lab

```bash
python3 clabonair.py deploy -t my-topology.clab.yml
```

If there is only one `.clab.yml` file in the current directory, you can omit `-t`:

```bash
python3 clabonair.py deploy
```

Once deployment completes, the script prints the simulation ID and a direct URL to the Nvidia AIR web interface.

### Checking lab status

```bash
python3 clabonair.py status --title my-lab
```

Or by simulation ID:

```bash
python3 clabonair.py status --sim-id <uuid>
```

This shows simulation state, nodes, interfaces, links, and SSH services.

### Destroying a lab

```bash
python3 clabonair.py destroy -t my-topology.clab.yml
```

Or by simulation ID:

```bash
python3 clabonair.py destroy --sim-id <uuid>
```

### Connecting to a node console (airconsole)

`airconsole.py` gives you an interactive SSH console to any node in a running simulation:

```bash
python3 airconsole.py
```

It lists all loaded simulations, lets you pick one, then lists the nodes in that simulation. Select a node and you get a live terminal session.

- For SR Linux nodes, default credentials (`admin` / `NokiaSrl1!`) are sent automatically.
- Press **Ctrl+]** to disconnect from the console.

You can also point it at a different credentials file:

```bash
python3 airconsole.py --vars /path/to/air.vars
```

## Using a different credentials file

Both scripts accept `--vars` to specify an alternate credentials file:

```bash
python3 clabonair.py deploy -t topo.clab.yml --vars /path/to/air.vars
python3 airconsole.py --vars /path/to/air.vars
```

By default, they look for `air.vars` in the current working directory.

## Debugging

### Debug logging

Pass `--debug` to clabonair to enable detailed logging:

```bash
python3 clabonair.py deploy -t my-topology.clab.yml --debug
```

This writes a verbose log to `clabonair-debug.log` in the script directory. The log includes timestamps, thread names, API calls, and command output from each VM, which is useful for diagnosing deployment failures.

### Common issues

**"No credentials" error** -- Your `air.vars` file is missing or incomplete. Make sure both `EMAIL` and `API_TOKEN` are set.

**"Authentication failed" (403)** -- Your API token may have expired. Generate a new one at <https://air.nvidia.com/account/api-tokens>.

**VM boot timeout** -- VMs have a 10-minute boot timeout by default. If Nvidia AIR is under heavy load, deployments may time out. Retry or check the AIR web console for simulation state.

**Image distribution failures** -- Container images are distributed between VMs using an HTTP-based peer-to-peer transfer. Check `clabonair-debug.log` for details on which node failed and at what stage.

**Console connection issues in airconsole** -- Make sure the simulation is in the LOADED state. Only loaded simulations expose console endpoints. If the connection drops, the WebSocket to AIR may have timed out; simply reconnect.

### Interrupting a deployment

You can press **Ctrl+C** during deployment. clabonair will signal background threads to stop and attempt cleanup of the partially created simulation.

## How it works

1. **Parse** -- Reads your containerlab YAML and extracts nodes, links, kinds, and images.
2. **Create** -- Builds a matching Nvidia AIR topology with Ubuntu VMs and inter-node links.
3. **Install** -- Boots the VMs, then installs Docker and containerlab on each in parallel.
4. **Distribute** -- Pre-loads container images across VMs using a peer-to-peer doubling tree (no registry VM needed).
5. **Deploy** -- Runs containerlab on each VM to bring up the individual node topologies with the correct macvlan links and startup configs.
