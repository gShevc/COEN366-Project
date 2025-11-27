#!/usr/bin/env python3
import socket
import threading
import json
import os
import time
import math

SERVER_HOST = "0.0.0.0"
SERVER_PORT = 5000  # UDP port for P2PBRS server

STATE_FILE = "server_state.json"
CHUNK_SIZE = 4096  # bytes (server-chosen chunk size)
HEARTBEAT_TIMEOUT = 60  # seconds, used only for logging


# Use a re-entrant lock because some handlers (e.g., REGISTER) call
# helper functions that also acquire the same lock.
lock = threading.RLock()

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
            # We keep file metadata (simulating backup still present somewhere)
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
            if p["role"] in ("STORAGE", "BOTH")
        ]

        if not storage_peers:
            return f"BACKUP-DENIED {rq} No-storage-peers"

        num_chunks = math.ceil(file_size / CHUNK_SIZE)
        if num_chunks == 0:
            num_chunks = 1

        # Round-robin assignment: each chunk gets one storage peer
        chunk_to_peers = {}
        peer_assignments = {}  # peer_name -> [chunk_ids]
        idx = 0
        for chunk_id in range(num_chunks):
            peer_name = storage_peers[idx % len(storage_peers)]
            idx += 1

            cid_str = str(chunk_id)
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
                if not p:
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
    else:
        rq = parts[1] if len(parts) > 1 else "0"
        resp = f"ERROR {rq} Unknown-command"

    if resp:
        sock.sendto(resp.encode(), addr)


def heartbeat_monitor():
    """Optional: periodically log peers that stopped sending heartbeats."""
    while True:
        time.sleep(HEARTBEAT_TIMEOUT)
        now = time.time()
        with lock:
            for name, p in peers.items():
                last = p.get("last_heartbeat", 0)
                if last and now - last > HEARTBEAT_TIMEOUT:
                    print(f"[SERVER] WARNING: Peer {name} missed heartbeat (last {int(now-last)} s ago)")


def run_server():
    load_state()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((SERVER_HOST, SERVER_PORT))
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
