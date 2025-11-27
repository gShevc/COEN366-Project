import socket
import sys
import threading
import time
import binascii


###############################################
# GLOBAL STATE
###############################################

RegisteredUsers = {}        # Name → {Role, IP, UDP_Port, TCP_Port, Capacity}
ServerRQ = 1                # Increment for any server-generated RQ
LOCK = threading.Lock()     # Thread safety

# Backup request state:
# rq → {
#    "ownerName": str,
#    "ownerAddr": (ip,udp),
#    "fileName": str,
#    "chunkSize": int,
#    "peers": [list of peer names],
#    "storeACK": set(),
#    "numChunks": int,
# }
BackupStates = {}

###############################################
# UTILITY
###############################################

def findChecksum(data_bytes):
    return binascii.crc32(data_bytes) & 0xFFFFFFFF


###############################################
# UDP SOCKET INIT
###############################################

def UDPConnection(self, HOST, PORT):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((HOST, PORT))
        print(f"Server UDP bound on {HOST}:{PORT}")
        return s
    except Exception as e:
        print("Socket bind failed:", e)
        sys.exit(1)


###############################################
# REGISTRATION
###############################################

def RegistrationCheck(data, addr, Name, Role, IP_Address, UDP_Port, TCP_Port, storageCapacity):
    global ServerRQ
    with LOCK:
        if Name not in RegisteredUsers:
            RegisteredUsers[Name] = {
                "Role": Role,
                "IP_Address": IP_Address,
                "UDP_Port": int(UDP_Port),
                "TCP_Port": int(TCP_Port),
                "Capacity": int(storageCapacity)
            }
            reply = f"REGISTERED|RQ{ServerRQ}|SUCCESS|"
            print(f"[REGISTER] User {Name} registered.")
        else:
            reply = f"REGISTER-DENIED|RQ{ServerRQ}|UserExists|"
            print(f"[REGISTER] User {Name} already exists.")

        ServerRQ += 1
        s.sendto(reply.encode("utf-8"), addr)


###############################################
# DEREGISTRATION
###############################################

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
# CHUNK SIZE SELECTION
###############################################

def differentChunkSizes(self, fileSize, minimumChunks):
    """
    Computes potential peers for storing file and chunk size.
    """
    fileSize = int(fileSize)
    newChunkSize = fileSize
    chunks = minimumChunks

    while True:
        available = []
        for name, info in RegisteredUsers.items():
            if info["Capacity"] >= newChunkSize:
                available.append(name)

        if len(available) >= chunks:
            return {
                "size": newChunkSize,
                "availablePeers": available,
                "requiredChunks": chunks,
            }

        newChunkSize = newChunkSize // 2
        chunks *= 2

        if newChunkSize <= 0:
            return None


###############################################
# STORAGE_TASK DISPATCH
###############################################

def sendStorageRequests(self, potentialPeers, rq, fileName, chunkSize, ownerAddr):
    """
    Sends STORAGE_TASK to each peer until enough ACCEPTED responses are gathered.
    """
    with LOCK:
        BackupStates[rq] = {
            "ownerName": None,  # assigned later in BACKUP_REQ
            "ownerAddr": ownerAddr,
            "fileName": fileName,
            "chunkSize": chunkSize,
            "peers": [],          # peers who accepted
            "storeACK": set(),
            "numChunks": potentialPeers["requiredChunks"],
        }

    # Send STORAGE_TASK to all peers
    for p in potentialPeers["availablePeers"]:
        peer = RegisteredUsers[p]
        msg = f"STORAGE_TASK|{rq}|{fileName}|{chunkSize}|{p}|"
        s.sendto(msg.encode("utf-8"), (peer["IP_Address"], peer["UDP_Port"]))

    print(f"[SERVER] Sent STORAGE_TASK to peers: {potentialPeers['availablePeers']}")


###############################################
# Generate BACKUP_PLAN
###############################################

def send_BACKUP_PLAN(rq):
    global ServerRQ

    with LOCK:
        st = BackupStates[rq]
        fileName = st["fileName"]
        chunkSize = st["chunkSize"]
        chosen = st["peers"]
        ownerAddr = st["ownerAddr"]

    # Build peer list including TCP connectivity information
    peerStrings = []
    for p in chosen:
        info = RegisteredUsers[p]
        entry = f"{p}:{info['IP_Address']}:{info['UDP_Port']}:{info['TCP_Port']}"
        peerStrings.append(entry)

    peerListStr = "[" + ",".join(peerStrings) + "]"

    msg = f"BACKUP_PLAN|RQ{ServerRQ}|{fileName}|{peerListStr}|{chunkSize}|"
    ServerRQ += 1

    print(f"[SERVER] Sending BACKUP_PLAN to owner at {ownerAddr}: {msg}")

    s.sendto(msg.encode("utf-8"), ownerAddr)


###############################################
# STORAGE_TASK ACCEPT/DENIED RESPONSE
###############################################

def handle_STORAGE_TASK_Response(parts, addr):
    """
    Receives STORAGE_TASK|RQ#|file|chunk|peerName|ACCEPTED| or ...|DENIED|
    """
    rq = parts[1]
    fileName = parts[2]
    chunk = parts[3]
    peerName = parts[4]
    decision = parts[5]

    if rq not in BackupStates:
        print("[ERROR] STORAGE_TASK response for unknown RQ")
        return

    with LOCK:
        st = BackupStates[rq]

        if decision == "ACCEPTED":
            st["peers"].append(peerName)
            print(f"[SERVER] Peer {peerName} ACCEPTED storage.")

        # if enough peers accepted, send final plan
        if len(st["peers"]) == st["numChunks"]:
            print("[SERVER] Enough peers accepted. Sending BACKUP_PLAN.")
            send_BACKUP_PLAN(rq)


###############################################
# STORE_ACK HANDLING
###############################################

def handle_STORE_ACK(parts, addr):
    """
    STORE_ACK|RQ#|FileName|Chunk_ID|peerName|
    """
    rq = parts[1]
    peerName = parts[4]

    with LOCK:
        if rq not in BackupStates:
            print("[STORE_ACK] Unknown RQ")
            return

        st = BackupStates[rq]
        st["storeACK"].add(peerName)

        print(f"[SERVER] Received STORE_ACK from {peerName} for rq={rq}. "
              f"{len(st['storeACK'])}/{len(st['peers'])}")

        if len(st["storeACK"]) == len(st["peers"]):
            print(f"[SERVER] All STORE_ACK received for RQ{rq}.")


###############################################
# BACKUP_DONE HANDLING
###############################################

def handle_BACKUP_DONE(parts, addr):
    rq = parts[1]
    fileName = parts[2]

    with LOCK:
        if rq not in BackupStates:
            print("[BACKUP_DONE] Unknown RQ")
            return

        st = BackupStates[rq]
        if len(st["storeACK"]) == len(st["peers"]):
            print(f"[SERVER] BACKUP_DONE: Full backup successful for file {fileName}.")
        else:
            print(f"[SERVER] BACKUP_DONE: Missing STORE_ACK(s). Incomplete backup.")

        # Cleanup
        del BackupStates[rq]


###############################################
# MAIN DISPATCH FOR EACH UDP MESSAGE
###############################################

def handle_packet(data, addr):
    data_string = data.decode("utf-8").strip()
    parts = data_string.strip('|').split('|')
    command = parts[0]

    # ----------------------
    # REGISTER
    # ----------------------
    if command == "REGISTER":
        _, rq, Name, Role, IP_Address, UDP_Port, TCP_Port, storageCapacity, *_ = parts
        RegistrationCheck(data, addr, Name, Role, IP_Address, UDP_Port, TCP_Port, storageCapacity)

    # ----------------------
    # DE-REGISTER
    # ----------------------
    elif command == "DE-REGISTER":
        _, rq, Name, *_ = parts
        Deregistration(data, addr, Name)

    # ----------------------
    # BACKUP_REQ
    # ----------------------
    elif command == "BACKUP_REQ":
        _, rq, fileName, fileSize, checksum = parts
        fileSize = int(fileSize)

        print(f"[SERVER] BACKUP_REQ from {addr} for file {fileName}")

        pot = differentChunkSizes("x", fileSize, 1)
        if pot is None:
            reply = f"BACKUP-DENIED|{rq}|NoPeersAvailable|"
            s.sendto(reply.encode("utf-8"), addr)
            return

        # Start sending STORAGE_TASK to determine who accepts
        sendStorageRequests("x", pot, rq, fileName, pot["size"], addr)

    # ----------------------
    # STORAGE_TASK RESPONSE
    # ----------------------
    elif command == "STORAGE_TASK":
        handle_STORAGE_TASK_Response(parts, addr)

    # ----------------------
    # STORE_ACK
    # ----------------------
    elif command == "STORE_ACK":
        handle_STORE_ACK(parts, addr)

    # ----------------------
    # BACKUP_DONE
    # ----------------------
    elif command == "BACKUP_DONE":
        handle_BACKUP_DONE(parts, addr)

    else:
        print(f"[SERVER] Unknown command from {addr}: {data_string}")


###############################################
# SERVER MAIN LOOP
###############################################

if __name__ == "__main__":
    HOST = "0.0.0.0"
    PORT = 8888

    s = UDPConnection("x", HOST, PORT)

    print("[SERVER] Running...")

    while True:
        data, addr = s.recvfrom(4096)
        threading.Thread(target=handle_packet, args=(data, addr), daemon=True).start()
