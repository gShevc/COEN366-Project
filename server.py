#!/usr/bin/env python3
import socket
import threading
import json
import os
import time
import math
import itertools

SERVER_HOST = "0.0.0.0"
SERVER_PORT = 5000  # UDP port for P2PBRS server

STATE_FILE = "server_state.json"
CHUNK_SIZE = 4096  # bytes (server-chosen chunk size)
HEARTBEAT_TIMEOUT = 60  # seconds, used only for logging


# Use a re-entrant lock because some handlers (e.g., REGISTER) call
# helper functions that also acquire the same lock.
lock = threading.RLock()
# UDP socket used by background threads to push control messages.
server_sock = None
# Simple server-side RQ# generator for internal messages (e.g., REPLICATE_REQ).
_server_rq_counter = itertools.count(100000)


def next_server_rq():
    return str(next(_server_rq_counter))


# Track chunks currently being replicated to avoid spamming requests.
replicating_chunks = set()  # (owner, file_name, chunk_id_str)

# In-memory state (persisted to JSON)
# peers: name -> {
#   "role": "OWNER"/"STORAGE"/"BOTH",
#   "ip": str,
#   "udp_port": int,
#   "tcp_port": int,
#   "storage_capacity": str,
#   "last_heartbeat": float (not persisted),
# }
peers = {}

# files: key "owner|file_name" -> {
#   "owner": str,
#   "file_name": str,
#   "size": int,
#   "checksum": str,   # full file checksum from BACKUP_REQ
#   "chunk_size": int,
#   "num_chunks": int,
#   "chunks": { "chunk_id_str": [peer_name, ...] }  # which peers host which chunk
# }
files = {}


def load_state():
    global peers, files
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
        peers = data.get("peers", {})
        files = data.get("files", {})
        # heartbeat is volatile (set to 0 on restart)
        for p in peers.values():
            p["last_heartbeat"] = 0.0
            p["alive"] = True
    else:
        peers = {}
        files = {}


def save_state():
    with lock:
        data = {
            "peers": peers,
            "files": files,
        }
        with open(STATE_FILE, "w") as f:
            json.dump(data, f, indent=2)


def peer_name_from_addr(addr):
    """Find registered peer name from UDP source (ip, port)."""
    ip, port = addr
    for name, p in peers.items():
        if p.get("ip") == ip and p.get("udp_port") == port:
            return name
    return None


def send_udp_to_peer(peer_name, message):
    """Send a control-plane UDP message to a specific peer if possible."""
    global server_sock
    if server_sock is None:
        return False
    p = peers.get(peer_name)
    if not p:
        return False
    try:
        server_sock.sendto(message.encode(), (p["ip"], p["udp_port"]))
        return True
    except OSError as e:
        print(f"[SERVER] ERROR sending to {peer_name}: {e}")
        return False


def remove_peer_from_chunks(peer_name):
    """Remove a peer from all chunk ownership lists and trigger replication."""
    with lock:
        for key, meta in files.items():
            owner = meta["owner"]
            file_name = meta["file_name"]
            for cid_str, hosts in list(meta["chunks"].items()):
                if peer_name in hosts:
                    hosts[:] = [h for h in hosts if h != peer_name]
                    if not hosts:
                        meta["chunks"][cid_str] = []
                    # Attempt to reallocate this chunk elsewhere.
                    trigger_replication(owner, file_name, cid_str, meta)
        save_state()


def select_target_peer(exclude):
    """Pick an alive storage-capable peer not in exclude."""
    for name, p in peers.items():
        if name in exclude:
            continue
        if p.get("role") not in ("STORAGE", "BOTH"):
            continue
        if not p.get("alive", True):
            continue
        return name
    return None


def trigger_replication(owner, file_name, cid_str, meta):
    """
    Ask a surviving host to replicate a chunk to another alive storage peer.
    meta is the file's metadata dict.
    """
    key = (owner, file_name, cid_str)
    hosts = meta["chunks"].get(cid_str, [])
    # Need at least one source to copy from.
    if not hosts:
        print(f"[SERVER] Unable to replicate {file_name} chunk {cid_str}: no surviving copy.")
        return

    # Avoid duplicate requests while one is in flight.
    if key in replicating_chunks:
        return

    # Choose a new target not already hosting the chunk.
    target = select_target_peer(exclude=set(hosts))
    if not target:
        return

    source = hosts[0]
    target_peer = peers[target]
    rq = next_server_rq()
    msg = f"REPLICATE_REQ {rq} {owner} {file_name} {cid_str} {target} {target_peer['ip']} {target_peer['tcp_port']}"
    if send_udp_to_peer(source, msg):
        replicating_chunks.add(key)
        print(f"[SERVER] Triggering replication of {file_name} chunk {cid_str} from {source} -> {target}")


def handle_register(parts, addr):
    # REGISTER RQ# Name Role IP_Address UDP_Port# TCP_Port# Storage_Capacity
    if len(parts) != 8:
        return f"REGISTER-DENIED {parts[1] if len(parts) > 1 else '0'} Invalid-REGISTER-format"

    _, rq, name, role, ip, udp_str, tcp_str, storage_cap = parts

    try:
        udp_port = int(udp_str)
        tcp_port = int(tcp_str)
    except ValueError:
        return f"REGISTER-DENIED {rq} Invalid-port"

    with lock:
        if name in peers:
            return f"REGISTER-DENIED {rq} Name-already-used"

        peers[name] = {
            "role": role.upper(),
            "ip": ip,
            "udp_port": udp_port,
            "tcp_port": tcp_port,
            "storage_capacity": storage_cap,
            "last_heartbeat": time.time(),
            "alive": True,
        }
        save_state()

    print(f"[SERVER] REGISTER: {name} role={role} ip={ip} udp={udp_port} tcp={tcp_port}")
    return f"REGISTERED {rq}"


def handle_deregister(parts, addr):
    # DE-REGISTER RQ# Name
    if len(parts) != 3:
        return f"ERROR {parts[1] if len(parts) > 1 else '0'} Invalid-DE-REGISTER-format"

    _, rq, name = parts

    with lock:
        if name in peers:
            print(f"[SERVER] DE-REGISTER: {name}")
            peers.pop(name)
            # Remove from chunk lists and attempt reallocation
            remove_peer_from_chunks(name)
            save_state()

    return f"DE-REGISTERED {rq}"


def handle_backup_req(parts, addr):
    # BACKUP_REQ RQ# File_Name File_Size Checksum
    if len(parts) != 5:
        return f"BACKUP-DENIED {parts[1] if len(parts) > 1 else '0'} Invalid-BACKUP_REQ-format"

    _, rq, file_name, size_str, checksum = parts

    owner = peer_name_from_addr(addr)
    if owner is None:
        return f"BACKUP-DENIED {rq} Not-registered"

    try:
        file_size = int(size_str)
    except ValueError:
        return f"BACKUP-DENIED {rq} Invalid-File_Size"

    with lock:
        # Select storage-capable peers (STORAGE or BOTH)
        storage_peers = [
            name for name, p in peers.items()
            if p["role"] in ("STORAGE", "BOTH") and p.get("alive", True)
        ]

        if not storage_peers:
            return f"BACKUP-DENIED {rq} No-storage-peers"

        num_chunks = math.ceil(file_size / CHUNK_SIZE)
        if num_chunks == 0:
            num_chunks = 1

        # Round-robin assignment: each chunk gets one or more storage peers (replicas)
        replicas = min(2, len(storage_peers))  # aim for 2-way redundancy when possible
        chunk_to_peers = {}
        peer_assignments = {}  # peer_name -> [chunk_ids]
        idx = 0
        for chunk_id in range(num_chunks):
            cid_str = str(chunk_id)
            for r in range(replicas):
                peer_name = storage_peers[idx % len(storage_peers)]
                idx += 1
                chunk_to_peers.setdefault(cid_str, []).append(peer_name)
                peer_assignments.setdefault(peer_name, []).append(chunk_id)

        key = f"{owner}|{file_name}"
        files[key] = {
            "owner": owner,
            "file_name": file_name,
            "size": file_size,
            "checksum": checksum,
            "chunk_size": CHUNK_SIZE,
            "num_chunks": num_chunks,
            "chunks": chunk_to_peers,
        }
        save_state()

        # Build Peer_List as:
        # [PeerA@ip:tcp:0,2;PeerB@ip:tcp:1,3]
        segs = []
        for peer_name, chunk_ids in peer_assignments.items():
            p = peers[peer_name]
            ids_str = ",".join(str(c) for c in chunk_ids)
            segs.append(f"{peer_name}@{p['ip']}:{p['tcp_port']}:{ids_str}")
        peer_list_str = "[" + ";".join(segs) + "]"

    print(f"[SERVER] BACKUP_REQ from {owner} for '{file_name}', {file_size} bytes, "
          f"{num_chunks} chunks -> peers {', '.join(peer_assignments.keys())}")
    return f"BACKUP_PLAN {rq} {file_name} {peer_list_str} {CHUNK_SIZE}"


def handle_store_ack(parts, addr):
    # STORE_ACK RQ# File_Name Chunk_ID
    if len(parts) != 4:
        return None  # no response needed

    _, rq, file_name, chunk_id_str = parts
    storage_peer = peer_name_from_addr(addr)
    if storage_peer is None:
        print("[SERVER] STORE_ACK from unknown peer (ignored)")
        return None

    cid_str = chunk_id_str
    with lock:
        # find file entry where this peer was supposed to store this chunk
        for meta in files.values():
            if meta["file_name"] != file_name:
                continue
            chunks = meta["chunks"].setdefault(cid_str, [])
            if storage_peer not in chunks:
                chunks.append(storage_peer)
            # Clear replication-in-flight marker if this chunk now has a host.
            key = (meta["owner"], file_name, cid_str)
            if key in replicating_chunks and chunks:
                replicating_chunks.discard(key)
        save_state()

    print(f"[SERVER] STORE_ACK: {storage_peer} stored chunk {cid_str} of {file_name}")
    return f"STORE_ACKED {rq}"


def handle_heartbeat(parts, addr):
    # HEARTBEAT RQ# Name Number_Chunks# Timestamp
    if len(parts) != 5:
        return None

    _, rq, name, num_chunks_str, timestamp = parts
    with lock:
        p = peers.get(name)
        if p:
            p["last_heartbeat"] = time.time()
            p["stored_chunks"] = int(num_chunks_str)
            p["alive"] = True
    # Optional heartbeat ACK
    return f"HEARTBEAT_ACK {rq} {name}"


def handle_restore_req(parts, addr):
    # RESTORE_REQ RQ# File_Name
    if len(parts) != 3:
        return f"RESTORE_FAIL {parts[1] if len(parts) > 1 else '0'} Invalid-RESTORE_REQ-format"

    _, rq, file_name = parts
    owner = peer_name_from_addr(addr)
    if owner is None:
        return f"RESTORE_FAIL {rq} {file_name} Not-registered"

    key = f"{owner}|{file_name}"
    with lock:
        meta = files.get(key)
        if not meta:
            return f"RESTORE_FAIL {rq} {file_name} Not-found"

        chunk_size = meta["chunk_size"]
        checksum = meta["checksum"]
        chunks = meta["chunks"]
        # Build assignments: Peer@ip:tcp:cid,cid,...
        peer_assign = {}
        for cid_str, peer_list in chunks.items():
            for peer_name in peer_list:
                p = peers.get(peer_name)
                if not p or not p.get("alive", True):
                    continue
                peer_assign.setdefault(peer_name, []).append(int(cid_str))

        if not peer_assign:
            return f"RESTORE_FAIL {rq} {file_name} No-available-chunks"

        segs = []
        for peer_name, cid_list in peer_assign.items():
            p = peers[peer_name]
            cid_list_sorted = sorted(cid_list)
            cid_strs = ",".join(str(c) for c in cid_list_sorted)
            segs.append(f"{peer_name}@{p['ip']}:{p['tcp_port']}:{cid_strs}")
        peer_list_str = "[" + ";".join(segs) + "]"

    print(f"[SERVER] RESTORE_REQ from {owner} for {file_name}")
    # add checksum at end so client can verify full file
    return f"RESTORE_PLAN {rq} {file_name} {peer_list_str} {chunk_size} {checksum}"


def handle_restore_result(parts, ok=True):
    # RESTORE_OK RQ# File_Name
    # RESTORE_FAIL RQ# File_Name Reason
    if ok and len(parts) >= 3:
        _, rq, file_name = parts[:3]
        print(f"[SERVER] RESTORE_OK for {file_name}")
        return f"RESTORE_OK_ACK {rq} {file_name}"
    elif not ok and len(parts) >= 4:
        _, rq, file_name, reason = parts[0], parts[1], parts[2], " ".join(parts[3:])
        print(f"[SERVER] RESTORE_FAIL for {file_name}: {reason}")
        return f"RESTORE_FAIL_ACK {rq} {file_name}"
    return None


def handle_backup_done(parts, addr):
    # BACKUP_DONE RQ# File_Name
    if len(parts) != 3:
        return f"ERROR {parts[1] if len(parts) > 1 else '0'} Invalid-BACKUP_DONE-format"

    _, rq, file_name = parts
    owner = peer_name_from_addr(addr) or "UNKNOWN"
    print(f"[SERVER] BACKUP_DONE from {owner} for {file_name}")
    return f"BACKUP_DONE_ACK {rq} {file_name}"


def handle_replicate_ack(parts):
    # REPLICATE_ACK RQ# Owner File_Name Chunk_ID STATUS [Reason]
    if len(parts) < 6:
        return None
    _, rq, owner, file_name, cid_str, status = parts[:6]
    reason = " ".join(parts[6:]) if len(parts) > 6 else ""
    key = (owner, file_name, cid_str)
    if key in replicating_chunks:
        replicating_chunks.discard(key)
    print(f"[SERVER] REPLICATE_ACK {status} for {file_name} chunk {cid_str} from owner {owner} {reason}")
    return None


def handle_message(sock, data, addr):
    msg = data.decode(errors="ignore").strip()
    if not msg:
        return

    print(f"[SERVER] From {addr}: {msg}")
    parts = msg.split()
    cmd = parts[0].upper()

    if cmd == "REGISTER":
        resp = handle_register(parts, addr)
    elif cmd == "DE-REGISTER":
        resp = handle_deregister(parts, addr)
    elif cmd == "BACKUP_REQ":
        resp = handle_backup_req(parts, addr)
    elif cmd == "STORE_ACK":
        resp = handle_store_ack(parts, addr)
    elif cmd == "HEARTBEAT":
        resp = handle_heartbeat(parts, addr)
    elif cmd == "RESTORE_REQ":
        resp = handle_restore_req(parts, addr)
    elif cmd == "RESTORE_OK":
        resp = handle_restore_result(parts, ok=True)
    elif cmd == "RESTORE_FAIL":
        resp = handle_restore_result(parts, ok=False)
    elif cmd == "BACKUP_DONE":
        resp = handle_backup_done(parts, addr)
    elif cmd == "REPLICATE_ACK":
        resp = handle_replicate_ack(parts)
    else:
        rq = parts[1] if len(parts) > 1 else "0"
        resp = f"ERROR {rq} Unknown-command"

    if resp:
        sock.sendto(resp.encode(), addr)


def heartbeat_monitor():
    """Periodically log peers that stopped sending heartbeats and trigger reallocation."""
    while True:
        time.sleep(HEARTBEAT_TIMEOUT)
        now = time.time()
        to_drop = []
        with lock:
            for name, p in peers.items():
                last = p.get("last_heartbeat", 0)
                if last and now - last > HEARTBEAT_TIMEOUT and p.get("alive", True):
                    print(f"[SERVER] WARNING: Peer {name} missed heartbeat (last {int(now-last)} s ago)")
                    p["alive"] = False
                    to_drop.append(name)
        for name in to_drop:
            remove_peer_from_chunks(name)


def run_server():
    load_state()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((SERVER_HOST, SERVER_PORT))
    global server_sock
    server_sock = sock
    print(f"[SERVER] P2PBRS server listening on UDP {SERVER_HOST}:{SERVER_PORT}")

    threading.Thread(target=heartbeat_monitor, daemon=True).start()

    try:
        while True:
            data, addr = sock.recvfrom(8192)
            t = threading.Thread(target=handle_message, args=(sock, data, addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("\n[SERVER] Shutting down...")
    finally:
        sock.close()
        save_state()


if __name__ == "__main__":
    run_server()
