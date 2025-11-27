import socket
import sys
import threading
import time
import binascii
import math

###############################################
# GLOBAL STATE
###############################################

RegisteredUsers = {}        # Name -> {Role, IP_Address, UDP_Port, TCP_Port, Capacity, last_seen}
ServerRQ = 1                # Increment for any server-generated RQ (use with LOCK)
LOCK = threading.Lock()     # Thread safety

# Constants for Heartbeat
HEARTBEAT_TIMEOUT = 15      # Seconds before a peer is considered dead
CHECK_INTERVAL = 5          # How often the server checks for dead peers

# Backup request state:
# rq -> {
#   "ownerName": str,
#   "ownerAddr": (ip,udp),
#   "fileName": str,
#   "chunkSize": int,
#   "peers": [list of peer names who accepted],
#   "storeACK": set(),
#   "numChunks": int,
# }
BackupStates = {}

# UDP socket (assigned in main)
s = None

###############################################
# UTILITIES
###############################################

def findChecksum(data_bytes):
    return binascii.crc32(data_bytes) & 0xFFFFFFFF

def findUserByAddr(addr):
    ip = addr[0]
    port = addr[1]

    for name, info in RegisteredUsers.items():
        # Normalize localhost values
        if info["IP_Address"] in ("127.0.0.1", "localhost") and ip in ("127.0.0.1", "localhost"):
            if int(info["UDP_Port"]) == port:
                return name

    return None


###############################################
# HEARTBEAT MONITOR
###############################################

def monitor_heartbeats():
    """
    Periodically checks if registered peers have sent a heartbeat recently.
    If a peer times out, it is removed.
    """
    print("[MONITOR] Heartbeat monitor thread started.")
    while True:
        time.sleep(CHECK_INTERVAL)
        
        current_time = time.time()
        to_remove = []

        with LOCK:
            for name, info in RegisteredUsers.items():
                last_seen = info.get("last_seen", current_time)
                
                # Check if timeout exceeded
                if current_time - last_seen > HEARTBEAT_TIMEOUT:
                    print(f"[MONITOR] Peer '{name}' timed out. (Last seen {current_time - last_seen:.1f}s ago)")
                    to_remove.append(name)

            # Remove dead peers
            for name in to_remove:
                # In Section 2.5, this is where REPLICATE_REQ would be triggered
                print(f"[FAILURE_HANDLING] Removing dead peer: {name}")
                del RegisteredUsers[name]
                
                # TODO (Section 2.5): Trigger Recovery/Replication here if this peer held chunks


###############################################
# UDP SOCKET INIT
###############################################

def UDPConnection( HOST, PORT):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((HOST, PORT))
        print(f"Server UDP bound on {HOST}:{PORT}")
        return sock
    except Exception as e:
        print("Socket bind failed:", e)
        sys.exit(1)

###############################################
# REGISTRATION / DEREGISTRATION
###############################################

def RegistrationCheck(data, addr, Name, Role, IP_Address, UDP_Port, TCP_Port, storageCapacity):
    global ServerRQ
    with LOCK:
        # Normalize capacity: accept "1024MB" or "1024" etc -> store integer bytes
        cap = storageCapacity
        try:
            # try to strip non-digits (e.g., "1024MB")
            digits = ''.join(ch for ch in str(storageCapacity) if ch.isdigit())
            cap_val = int(digits) if digits != '' else int(storageCapacity)
        except Exception:
            cap_val = 0

        if Name not in RegisteredUsers:
            RegisteredUsers[Name] = {
                "Role": Role,
                "IP_Address": IP_Address,
                "UDP_Port": int(UDP_Port),
                "TCP_Port": int(TCP_Port),
                "Capacity": int(cap_val),
                "socket": (IP_Address, int(UDP_Port)),
                "last_seen": time.time() # Initialize heartbeat timestamp
            }
            reply = f"REGISTERED|RQ{ServerRQ}|SUCCESS|"
            print(f"[REGISTER] User {Name} registered. Info: {RegisteredUsers[Name]}")
        else:
            reply = f"REGISTER-DENIED|RQ{ServerRQ}|UserExists|"
            print(f"[REGISTER] User {Name} already exists.")

        ServerRQ += 1
        s.sendto(reply.encode("utf-8"), addr)


def Deregistration(data, addr, Name):
    global ServerRQ
    with LOCK:
        if Name in RegisteredUsers:
            del RegisteredUsers[Name]
            reply = f"DE-REGISTERED|RQ{ServerRQ}|SUCCESS|"
            print(f"[DE-REGISTER] Removed user {Name}.")
        else:
            reply = f"DE-REGISTER-DENIED|RQ{ServerRQ}|NoSuchUser|"
            print(f"[DE-REGISTER] User {Name} not registered.")

        ServerRQ += 1
        s.sendto(reply.encode("utf-8"), addr)

###############################################
# CHUNK SIZE SELECTION (corrected)
###############################################

def differentChunkSizes(fileSize, minimumChunks, ownerName):
    """
    Compute a chunkSize and a list of available peers excluding ownerName.
    - Try chunks = minimumChunks, minimumChunks+1, ..., up to number of eligible peers.
    - Use ceil division for chunkSize so all bytes are accounted for.
    Returns dict with {"size": chunkSize, "availablePeers": [...], "requiredChunks": chunks}
    or None if impossible.
    """
    fileSize = int(fileSize)
    chunks = int(minimumChunks)

    # Build eligible peers dict excluding owner and only STORAGE/BOTH roles
    eligiblePeers = {
        name: info
        for name, info in RegisteredUsers.items()
        if name != ownerName and info.get("Role", "").upper() in ("STORAGE", "BOTH")
    }

    if not eligiblePeers:
        return None

    maxPeers = len(eligiblePeers)

    while chunks <= maxPeers:
        # compute chunk size (ceil)
        chunkSize = math.ceil(fileSize / chunks)
        available = []
        for name, info in eligiblePeers.items():
            # ensure numeric capacity comparison and correct logical AND
            try:
                if int(info.get("Capacity", 0)) >= int(chunkSize):
                    available.append(name)
            except Exception:
                # if capacity malformed, skip peer
                continue

        if len(available) >= chunks:
            return {
                "size": int(chunkSize),
                "availablePeers": available,
                "requiredChunks": int(chunks),
            }

        chunks += 1

    return None

###############################################
# STORAGE_TASK DISPATCH
###############################################

def sendStorageRequests(potentialPeers, rq, fileName, chunkSize, ownerName):
    """
    Sends STORAGE_TASK to candidate peers and sets up BackupStates entry.
    When enough peers reply ACCEPTED (handled elsewhere), we'll call send_BACKUP_PLAN(rq).
    """
    ownerAddr = RegisteredUsers[ownerName]["socket"]

    with LOCK:
        BackupStates[rq] = {
            "ownerName": None,
            "ownerAddr": ownerAddr,
            "fileName": fileName,
            "chunkSize": int(chunkSize),
            "peers": [],          # peers who accepted (names)
            "storeACK": set(),
            "numChunks": potentialPeers["requiredChunks"],
        }

    # Send STORAGE_TASK to all potential peers (only those in availablePeers)
    for p in potentialPeers["availablePeers"]:
        peer = RegisteredUsers[p]
        if not peer:
            continue
        msg = f"STORAGE_TASK|{rq}|{fileName}|{chunkSize}|{p}|"
        try:
            s.sendto(msg.encode("utf-8"), (peer["IP_Address"], (peer["UDP_Port"])))
        except Exception as e:
            print(f"[SERVER] Failed to send STORAGE_TASK to {p}: {e}")

    print(f"[SERVER] Sent STORAGE_TASK to peers: {potentialPeers['availablePeers']} for rq={rq}")
    print(f"[SERVER] Owner for rq={rq} is {ownerName} at {ownerAddr}")

###############################################
# Generate BACKUP_PLAN (use original rq)
###############################################

def send_BACKUP_PLAN(rq):
    #with LOCK:
    if rq not in BackupStates:
        print(f"[SERVER] send_BACKUP_PLAN: unknown rq {rq}")
        return
    else:
        print("[SERVER] Generating BACKUP_PLAN for rq", rq)
        st = BackupStates[rq]
        fileName = st["fileName"]
        chunkSize = st["chunkSize"]
        chosen = st["peers"]
        ownerAddr = st["ownerAddr"]

    if (len(chosen) ==0) :
        print(f"[SERVER] send_BACKUP_PLAN: no peers chosen for rq {rq}")
    
    peerStrings = [] # Build peer list with connection info
    for p in chosen:
        print(f"[SERVER] Adding peer {p} to BACKUP_PLAN for rq={rq}")
        info = RegisteredUsers[p]
        if not info:
            continue
        entry = f"{p}:{info['IP_Address']}:{info['UDP_Port']}:{info['TCP_Port']}"
        peerStrings.append(entry)

    peerListStr = "[" + ",".join(peerStrings) + "]"
    msg = f"BACKUP_PLAN|{rq}|{fileName}|{peerListStr}|{chunkSize}|"

    print(f"[SERVER] Sending BACKUP_PLAN to owner at {ownerAddr}: {msg}")
    s.sendto(msg.encode("utf-8"), ownerAddr)

###############################################
# STORAGE_TASK responses
###############################################

def handle_STORAGE_TASK_Response(parts):
    """
    Expecting: STORAGE_TASK|RQ#|file|chunk|peerName|ACCEPTED|  (or DENIED)
    """
    if len(parts) < 6:
        print("[SERVER] Malformed STORAGE_TASK response:", parts)
        return
    rq = parts[1]
    fileName = parts[2]
    chunk = parts[3]
    peerName = parts[4]
    decision = parts[5].upper()

    if rq not in BackupStates:
        print("[ERROR] STORAGE_TASK response for unknown RQ", rq)
        return

    with LOCK:
        st = BackupStates[rq]
        if decision == "ACCEPTED":
            if peerName not in st["peers"]:
                st["peers"].append(peerName)
            print(f"[SERVER] Peer {peerName} ACCEPTED storage for rq={rq}. total accepted={len(st['peers'])}/{st['numChunks']}")

        else:
            print(f"[SERVER] Peer {peerName} DENIED storage for rq={rq}.")

        # if enough peers accepted, send final plan
        if len(st["peers"]) >= st["numChunks"]:
            print(f"[SERVER] Enough peers accepted for rq={rq}. Sending BACKUP_PLAN.")
            send_BACKUP_PLAN(rq)

###############################################
# STORE_ACK / BACKUP_DONE handlers
###############################################

def handle_STORE_ACK(parts):
    """
    STORE_ACK|RQ#|FileName|Chunk_ID|peerName|
    """
    if len(parts) < 5:
        return
    rq = parts[1]
    peerName = parts[4]

    with LOCK:
        if rq not in BackupStates:
            print("[STORE_ACK] Unknown RQ", rq)
            return
        st = BackupStates[rq]
        st["storeACK"].add(peerName)
        print(f"[SERVER] Received STORE_ACK from {peerName} for rq={rq}. ({len(st['storeACK'])}/{len(st['peers'])})")
        if len(st["storeACK"]) >= len(st["peers"]):
            print(f"[SERVER] All STORE_ACK received for RQ{rq}.")

def handle_BACKUP_DONE(parts):
    if len(parts) < 3:
        return
    rq = parts[1]
    fileName = parts[2]

    with LOCK:
        if rq not in BackupStates:
            print("[BACKUP_DONE] Unknown RQ", rq)
            return
        st = BackupStates[rq]
        if len(st["storeACK"]) >= len(st["peers"]):
            print(f"[SERVER] BACKUP_DONE: Full backup successful for file {fileName}.")
        else:
            print(f"[SERVER] BACKUP_DONE: Missing STORE_ACK(s). Incomplete backup for file {fileName}.")
        # Cleanup
        del BackupStates[rq]

###############################################
# HEARTBEAT HANDLER
###############################################

def handle_HEARTBEAT(parts, addr):
    # Format: HEARTBEAT|RQ#|Name|Number_Chunks#|Timestamp|
    if len(parts) < 5:
        return

    name = parts[2]
    
    with LOCK:
        if name in RegisteredUsers:
            RegisteredUsers[name]["last_seen"] = time.time()
            RegisteredUsers[name]["chunks_count"] = parts[3]
            # print(f"[HEARTBEAT] Received from {name}") # Uncomment to debug
        else:
            pass

###############################################
# Main UDP packet dispatcher
###############################################

def handle_packet(data, addr):
    msg = data.decode("utf-8").strip().strip("|")
    parts = msg.split("|")
    cmd = parts[0]

    if cmd == "REGISTER":
        _, rq, Name, Role, IP, UDP, TCP, Capacity = parts
        RegistrationCheck(data, addr, Name, Role, IP, UDP, TCP, Capacity)

    elif cmd == "DE-REGISTER":
        _, rq, Name = parts
        Deregistration(data, addr, Name)
        
    elif cmd == "HEARTBEAT":
        handle_HEARTBEAT(parts, addr)

    elif cmd == "BACKUP_REQ":
        _, rq, fileName, fileSize, checksum = parts
        print(f"[SERVER] BACKUP_REQ from {addr} for {fileName}")

        # determine requester name by exact UDP match
        ownerName = None
        for name, info in RegisteredUsers.items():
            if info["UDP_Port"] == addr[1]:
                ownerName = name
                break

        if ownerName is None:
            print(f"[SERVER] ERROR: Cannot match BACKUP_REQ sender {addr} to any registered user.")
            return

        pot = differentChunkSizes(int(fileSize), 1, ownerName)
        if pot is None:
            s.sendto(f"BACKUP-DENIED|{rq}|NoPeers|".encode(), addr)
            return

        sendStorageRequests(pot, rq, fileName, pot["size"], ownerName)

    elif cmd == "STORAGE_TASK":
        handle_STORAGE_TASK_Response(parts)

    elif cmd == "STORE_ACK":
        handle_STORE_ACK(parts)

    elif cmd == "BACKUP_DONE":
        handle_BACKUP_DONE(parts)

    else:
        print(f"[SERVER] Unknown command: {msg} from {addr}")

###############################################
# Main
###############################################

if __name__ == "__main__":
    HOST = "0.0.0.0"
    PORT = 8888

    s = UDPConnection(HOST, PORT)
    print("[SERVER] Running...")
    
    # Start the Heartbeat Monitor Thread
    threading.Thread(target=monitor_heartbeats, daemon=True).start()

    while True:
        data, addr = s.recvfrom(8192)
        threading.Thread(target=handle_packet, args=(data, addr), daemon=True).start()