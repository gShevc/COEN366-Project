#!/usr/bin/env python3
import argparse
import os
import socket
import threading
import time
import sys
import zlib

SERVER_DEFAULT_PORT = 5000


def get_local_ip(probe_host="8.8.8.8", probe_port=53):
    """Best-effort outward-facing IP discovery."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((probe_host, probe_port))
        ip = s.getsockname()[0]
    except OSError:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


class P2PBRSClient:
    """
    Peer for the Peer-to-Peer Backup & Recovery System.

    - Control plane: UDP with server
      REGISTER, DE-REGISTER, BACKUP_REQ, RESTORE_REQ, HEARTBEAT, STORE_ACK, etc.

    - Data plane: TCP peer-to-peer
      SEND_CHUNK (owner -> storage peer)
      GET_CHUNK / CHUNK_DATA (storage peer -> owner)
    """

    def __init__(
        self,
        name,
        role,
        server_host,
        server_port,
        udp_port,
        tcp_port,
        storage_capacity,
        base_dir,
    ):
        self.name = name
        self.role = role.upper()
        self.server_host = server_host
        self.server_port = server_port
        self.udp_port = udp_port
        self.tcp_port = tcp_port
        self.storage_capacity = storage_capacity

        self.local_ip = get_local_ip(server_host)

        # Directories
        self.base_dir = os.path.abspath(base_dir)
        self.owner_dir = os.path.join(self.base_dir, "owner_files")
        self.storage_dir = os.path.join(self.base_dir, "storage_chunks")
        self.restore_dir = os.path.join(self.base_dir, "restored_files")
        for d in (self.owner_dir, self.storage_dir, self.restore_dir):
            os.makedirs(d, exist_ok=True)

        # UDP socket (bound to our registered control port)
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind(("0.0.0.0", self.udp_port))
        self.udp_sock.settimeout(5.0)

        # Request sequence number
        self.rq = 1

        self.running = True

    # ---------------- UDP helpers ----------------

    def next_rq(self):
        rq = self.rq
        self.rq += 1
        return rq

    def send_udp_request(self, msg, verbose=True):
        """
        Send a UDP request to server and wait for a single reply.
        (No concurrent requests in this simple client.)
        """
        if verbose:
            print(f"[{self.name}] --> SERVER: {msg}")
        self.udp_sock.sendto(msg.encode(), (self.server_host, self.server_port))
        try:
            data, _ = self.udp_sock.recvfrom(8192)
            resp = data.decode(errors="ignore").strip()
            if verbose:
                print(f"[{self.name}] <-- SERVER: {resp}")
            return resp
        except socket.timeout:
            if verbose:
                print(f"[{self.name}] ERROR: no response from server (timeout)")
            return None

    # ---------------- Protocol operations ----------------

    def register(self):
        rq = self.next_rq()
        msg = (
            f"REGISTER {rq} {self.name} {self.role} "
            f"{self.local_ip} {self.udp_port} {self.tcp_port} {self.storage_capacity}"
        )
        resp = self.send_udp_request(msg)
        if not resp:
            return False
        if resp.split()[0] == "REGISTERED":
            print(f"[{self.name}] Registered successfully.")
            return True
        print(f"[{self.name}] REGISTER failed: {resp}")
        return False

    def deregister(self):
        rq = self.next_rq()
        msg = f"DE-REGISTER {rq} {self.name}"
        resp = self.send_udp_request(msg)
        print(f"[{self.name}] De-register response: {resp}")

    def heartbeat_loop(self):
        while self.running:
            try:
                time.sleep(10)
                rq = self.next_rq()
                num_chunks = self.count_chunks()
                timestamp = int(time.time())
                msg = f"HEARTBEAT {rq} {self.name} {num_chunks} {timestamp}"
                # Suppress heartbeat chatter so the CLI stays clean for commands.
                self.send_udp_request(msg, verbose=False)
            except Exception as e:
                print(f"[{self.name}] HEARTBEAT error: {e}")

    def count_chunks(self):
        return len(
            [f for f in os.listdir(self.storage_dir) if f.endswith(".chunk")]
        )

    # ----- File backup (owner side) -----

    def backup_file(self, filename):
        filepath = os.path.join(self.owner_dir, filename)
        if not os.path.isfile(filepath):
            print(f"[{self.name}] File not found in owner_files: {filename}")
            return

        with open(filepath, "rb") as f:
            data = f.read()

        file_size = len(data)
        checksum = "%08x" % (zlib.crc32(data) & 0xFFFFFFFF)

        rq = self.next_rq()
        msg = f"BACKUP_REQ {rq} {filename} {file_size} {checksum}"
        resp = self.send_udp_request(msg)
        if not resp:
            return

        parts = resp.split(maxsplit=4)
        if parts[0] == "BACKUP-DENIED":
            print(f"[{self.name}] Backup denied: {resp}")
            return
        if len(parts) < 5 or parts[0] != "BACKUP_PLAN":
            print(f"[{self.name}] Unexpected BACKUP response: {resp}")
            return

        _, plan_rq, plan_file, peer_list_raw, chunk_size_str = parts
        try:
            chunk_size = int(chunk_size_str)
        except ValueError:
            print(f"[{self.name}] Invalid chunk size from server.")
            return

        # Peer_List format: [Peer@ip:tcp:0,1;Peer2@ip:tcp:2,3]
        peer_list_raw = peer_list_raw.strip()
        if not (peer_list_raw.startswith("[") and peer_list_raw.endswith("]")):
            print(f"[{self.name}] Invalid peer list format: {peer_list_raw}")
            return
        inner = peer_list_raw[1:-1]
        if not inner.strip():
            print(f"[{self.name}] Empty peer list.")
            return

        assignments = []  # list of (peer_name, ip, tcp, [chunk_ids])
        for seg in inner.split(";"):
            seg = seg.strip()
            if not seg:
                continue
            try:
                name_part, after_at = seg.split("@", 1)
                addr_part, chunk_ids_part = after_at.split(":", 2)[0:2], after_at.split(":", 2)[2]
                # after_at is "ip:tcp:ids"
                ip, tcp_str = addr_part
                tcp_port = int(tcp_str)
                chunk_ids = [int(c) for c in chunk_ids_part.split(",") if c]
                assignments.append((name_part, ip, tcp_port, chunk_ids))
            except Exception:
                print(f"[{self.name}] Failed to parse segment: {seg}")
                return

        # Actually send chunks via TCP
        print(f"[{self.name}] Starting TCP chunk uploads...")
        for peer_name, ip, port, chunk_ids in assignments:
            for cid in chunk_ids:
                offset = cid * chunk_size
                chunk = data[offset: offset + chunk_size]
                self.send_chunk_to_peer(ip, port, filename, cid, chunk)

        # Notify server that backup is logically complete
        rq2 = self.next_rq()
        done_msg = f"BACKUP_DONE {rq2} {filename}"
        # Spec: BACKUP_DONE is a UDP control message
        self.send_udp_request(done_msg)

    def send_chunk_to_peer(self, peer_ip, peer_tcp_port, filename, chunk_id, chunk_bytes):
        """
        TCP: SEND_CHUNK RQ# File_Name Chunk_ID Chunk_Size Checksum
        <chunk_bytes>
        Peer responds with: CHUNK_OK ... or CHUNK_ERROR ...
        """
        attempts = 0
        while attempts < 3:
            attempts += 1
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((peer_ip, peer_tcp_port))
                f = s.makefile("rwb")

                rq = self.next_rq()
                chunk_size = len(chunk_bytes)
                checksum = "%08x" % (zlib.crc32(chunk_bytes) & 0xFFFFFFFF)
                header = f"SEND_CHUNK {rq} {filename} {chunk_id} {chunk_size} {checksum}\n"
                f.write(header.encode())
                f.write(chunk_bytes)
                f.flush()

                # response line
                line = f.readline().decode(errors="ignore").strip()
                print(f"[{self.name}] <- {peer_ip}:{peer_tcp_port} {line}")
                if line.startswith("CHUNK_OK"):
                    print(
                        f"[{self.name}] Chunk {chunk_id} of {filename} stored successfully on "
                        f"{peer_ip}:{peer_tcp_port}"
                    )
                    return
                else:
                    print(
                        f"[{self.name}] Chunk {chunk_id} error from peer, attempt {attempts}: {line}"
                    )
            except OSError as e:
                print(
                    f"[{self.name}] Error sending chunk {chunk_id} to {peer_ip}:{peer_tcp_port} "
                    f"(attempt {attempts}): {e}"
                )
            finally:
                try:
                    s.close()
                except Exception:
                    pass

        print(f"[{self.name}] Giving up on chunk {chunk_id} after 3 attempts.")

    # ----- Storage side: TCP server -----

    def chunk_server_loop(self):
        """
        TCP server for SEND_CHUNK and GET_CHUNK.
        """
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.tcp_port))
        srv.listen(5)
        print(f"[{self.name}] Chunk server listening on TCP {self.local_ip}:{self.tcp_port}")

        while self.running:
            try:
                conn, addr = srv.accept()
            except OSError:
                break
            t = threading.Thread(
                target=self.handle_chunk_connection, args=(conn, addr), daemon=True
            )
            t.start()

        srv.close()

    def chunk_path(self, file_name, chunk_id):
        safe_name = file_name.replace("/", "_").replace("\\", "_")
        return os.path.join(self.storage_dir, f"{safe_name}.{chunk_id}.chunk")

    def handle_chunk_connection(self, conn, addr):
        try:
            f = conn.makefile("rwb")
            header = f.readline().decode(errors="ignore").strip()
            if not header:
                return
            parts = header.split()
            cmd = parts[0].upper()
            if cmd == "SEND_CHUNK":
                # SEND_CHUNK RQ# File_Name Chunk_ID Chunk_Size Checksum
                if len(parts) != 6:
                    f.write(b"CHUNK_ERROR 0 Invalid-header\n")
                    f.flush()
                    return
                _, rq, file_name, cid_str, size_str, checksum = parts
                try:
                    chunk_id = int(cid_str)
                    chunk_size = int(size_str)
                except ValueError:
                    f.write(b"CHUNK_ERROR 0 Invalid-size\n")
                    f.flush()
                    return

                data = b""
                remaining = chunk_size
                while remaining > 0:
                    chunk = f.read(min(4096, remaining))
                    if not chunk:
                        break
                    data += chunk
                    remaining -= len(chunk)

                if len(data) != chunk_size:
                    f.write(b"CHUNK_ERROR 0 Incomplete-data\n")
                    f.flush()
                    return

                calc = "%08x" % (zlib.crc32(data) & 0xFFFFFFFF)
                if calc != checksum:
                    f.write(
                        f"CHUNK_ERROR {rq} {file_name} {chunk_id} Checksum_Mismatch\n".encode()
                    )
                    f.flush()
                    return

                # Store chunk on disk
                path = self.chunk_path(file_name, chunk_id)
                with open(path, "wb") as out:
                    out.write(data)

                # Acknowledge to sender
                f.write(f"CHUNK_OK {rq} {file_name} {chunk_id}\n".encode())
                f.flush()

                # Notify server: STORE_ACK
                rq2 = self.next_rq()
                msg = f"STORE_ACK {rq2} {file_name} {chunk_id}"
                self.send_udp_request(msg)

                print(
                    f"[{self.name}] Stored chunk {chunk_id} of {file_name} from {addr}, "
                    f"path={path}"
                )

            elif cmd == "GET_CHUNK":
                # GET_CHUNK RQ# File_Name Chunk_ID
                if len(parts) != 4:
                    f.write(b"CHUNK_ERROR 0 Invalid-GET_CHUNK\n")
                    f.flush()
                    return
                _, rq, file_name, cid_str = parts
                try:
                    chunk_id = int(cid_str)
                except ValueError:
                    f.write(b"CHUNK_ERROR 0 Invalid-chunk-id\n")
                    f.flush()
                    return

                path = self.chunk_path(file_name, chunk_id)
                if not os.path.isfile(path):
                    f.write(
                        f"CHUNK_ERROR {rq} {file_name} {chunk_id} Not_Found\n".encode()
                    )
                    f.flush()
                    return

                with open(path, "rb") as src:
                    data = src.read()
                checksum = "%08x" % (zlib.crc32(data) & 0xFFFFFFFF)
                # CHUNK_DATA RQ# File_Name Chunk_ID Checksum
                f.write(
                    f"CHUNK_DATA {rq} {file_name} {chunk_id} {checksum}\n".encode()
                )
                f.write(data)
                f.flush()
                print(f"[{self.name}] Served chunk {chunk_id} of {file_name} to {addr}")

            else:
                f.write(b"CHUNK_ERROR 0 Unknown-command\n")
                f.flush()
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ----- Restore (owner side) -----

    def restore_file(self, filename):
        rq = self.next_rq()
        msg = f"RESTORE_REQ {rq} {filename}"
        resp = self.send_udp_request(msg)
        if not resp:
            return

        parts = resp.split(maxsplit=5)
        if parts[0] == "RESTORE_FAIL":
            print(f"[{self.name}] Restore failed: {resp}")
            return
        if len(parts) < 6 or parts[0] != "RESTORE_PLAN":
            print(f"[{self.name}] Unexpected RESTORE response: {resp}")
            return

        _, plan_rq, plan_file, peer_list_raw, chunk_size_str, checksum = parts
        try:
            chunk_size = int(chunk_size_str)
        except ValueError:
            print(f"[{self.name}] Invalid chunk size from server.")
            return

        # Peer list same format as BACKUP_PLAN: [Peer@ip:tcp:0,1;...]
        inner = peer_list_raw.strip()
        if not (inner.startswith("[") and inner.endswith("]")):
            print(f"[{self.name}] Invalid peer list: {peer_list_raw}")
            return
        inner = inner[1:-1]

        assignments = []  # (peer_name, ip, tcp, [chunk_ids])
        for seg in inner.split(";"):
            seg = seg.strip()
            if not seg:
                continue
            try:
                name_part, after_at = seg.split("@", 1)
                addr_part, chunk_ids_part = after_at.split(":", 2)[0:2], after_at.split(":", 2)[2]
                ip, tcp_str = addr_part
                tcp_port = int(tcp_str)
                chunk_ids = [int(c) for c in chunk_ids_part.split(",") if c]
                assignments.append((name_part, ip, tcp_port, chunk_ids))
            except Exception:
                print(f"[{self.name}] Failed to parse restore segment: {seg}")
                return

        # Build map chunk_id -> (ip, tcp)
        chunk_sources = {}
        for peer_name, ip, port, cids in assignments:
            for cid in cids:
                chunk_sources[cid] = (ip, port)

        # Download all chunks
        chunks = {}
        for cid, (ip, port) in chunk_sources.items():
            chunk = self.fetch_chunk_from_peer(ip, port, filename, cid)
            if chunk is None:
                print(f"[{self.name}] Failed to retrieve chunk {cid}")
                continue
            chunks[cid] = chunk

        if not chunks:
            print(f"[{self.name}] No chunks retrieved; cannot restore.")
            # notify server
            rq2 = self.next_rq()
            msg_fail = f"RESTORE_FAIL {rq2} {filename} No_chunks_retrieved"
            self.send_udp_request(msg_fail)
            return

        # Reassemble in order from chunk 0 upwards until a gap
        max_cid = max(chunks.keys())
        data = b""
        for cid in range(max_cid + 1):
            if cid not in chunks:
                print(f"[{self.name}] Missing chunk {cid}, aborting restore.")
                rq2 = self.next_rq()
                msg_fail = f"RESTORE_FAIL {rq2} {filename} Missing_chunk_{cid}"
                self.send_udp_request(msg_fail)
                return
            data += chunks[cid]

        calc_full = "%08x" % (zlib.crc32(data) & 0xFFFFFFFF)
        if calc_full != checksum:
            print(
                f"[{self.name}] Restore checksum mismatch: expected {checksum}, got {calc_full}"
            )
            rq2 = self.next_rq()
            msg_fail = f"RESTORE_FAIL {rq2} {filename} Checksum_mismatch"
            self.send_udp_request(msg_fail)
            return

        out_path = os.path.join(self.restore_dir, filename)
        with open(out_path, "wb") as out:
            out.write(data)
        print(f"[{self.name}] Restore successful, wrote {out_path}")

        rq2 = self.next_rq()
        msg_ok = f"RESTORE_OK {rq2} {filename}"
        self.send_udp_request(msg_ok)

    def fetch_chunk_from_peer(self, ip, port, filename, chunk_id):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((ip, port))
            f = s.makefile("rwb")
            rq = self.next_rq()
            header = f"GET_CHUNK {rq} {filename} {chunk_id}\n"
            f.write(header.encode())
            f.flush()

            line = f.readline().decode(errors="ignore").strip()
            if not line:
                print(f"[{self.name}] No CHUNK_DATA header from {ip}:{port}")
                return None
            parts = line.split()
            if parts[0] != "CHUNK_DATA":
                print(f"[{self.name}] GET_CHUNK error from {ip}:{port}: {line}")
                return None
            _, _, _, cid_str, checksum = parts
            try:
                cid = int(cid_str)
            except ValueError:
                print(f"[{self.name}] Invalid chunk id in CHUNK_DATA: {line}")
                return None

            data = f.read()  # read until EOF (one chunk per connection)
            calc = "%08x" % (zlib.crc32(data) & 0xFFFFFFFF)
            if calc != checksum:
                print(
                    f"[{self.name}] Chunk checksum mismatch from {ip}:{port}: "
                    f"expected {checksum}, got {calc}"
                )
                return None
            print(f"[{self.name}] Retrieved chunk {cid} from {ip}:{port}")
            return data
        except OSError as e:
            print(f"[{self.name}] GET_CHUNK error from {ip}:{port}: {e}")
            return None
        finally:
            try:
                s.close()
            except Exception:
                pass

    # ------------- CLI loop -------------

    def cli_loop(self):
        print("Commands:")
        print("  backup <filename>   - backup file from owner_files/")
        print("  restore <filename>  - restore file into restored_files/")
        print("  list-owner          - list files in owner_files/")
        print("  list-storage        - list stored chunks/")
        print("  quit                - de-register and exit")
        print()

        while True:
            try:
                line = input(f"{self.name}> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                line = "quit"

            if not line:
                continue
            parts = line.split()
            cmd = parts[0].lower()

            if cmd == "backup":
                if len(parts) < 2:
                    print("Usage: backup <filename>")
                    continue
                self.backup_file(" ".join(parts[1:]))
            elif cmd == "restore":
                if len(parts) < 2:
                    print("Usage: restore <filename>")
                    continue
                self.restore_file(" ".join(parts[1:]))
            elif cmd == "list-owner":
                print("Owner files:")
                for f in os.listdir(self.owner_dir):
                    print("  ", f)
            elif cmd == "list-storage":
                print("Stored chunks:")
                for f in os.listdir(self.storage_dir):
                    print("  ", f)
            elif cmd in ("quit", "exit"):
                self.running = False
                self.deregister()
                break
            else:
                print("Unknown command.")

    # ------------- Main run -------------

    def run(self):
        if not self.register():
            print(f"[{self.name}] Could not register, exiting.")
            return

        # Start background TCP chunk server
        t_srv = threading.Thread(target=self.chunk_server_loop, daemon=True)
        t_srv.start()

        # Start heartbeat thread
        t_hb = threading.Thread(target=self.heartbeat_loop, daemon=True)
        t_hb.start()

        try:
            self.cli_loop()
        finally:
            self.running = False
            try:
                self.udp_sock.close()
            except Exception:
                pass
            print(f"[{self.name}] Bye.")
            time.sleep(0.5)


def parse_args():
    p = argparse.ArgumentParser(description="P2PBRS Peer")
    p.add_argument("--name", required=True, help="Unique peer name")
    p.add_argument("--role", required=True, choices=["OWNER", "STORAGE", "BOTH"])
    p.add_argument("--server-host", default="127.0.0.1")
    p.add_argument("--server-port", type=int, default=SERVER_DEFAULT_PORT)
    p.add_argument("--udp-port", type=int, required=True, help="UDP control port")
    p.add_argument("--tcp-port", type=int, required=True, help="TCP chunk port")
    p.add_argument("--storage-capacity", default="1024MB")
    p.add_argument("--base-dir", default="./peer_data", help="Base directory for this peer")
    return p.parse_args()


def main():
    args = parse_args()
    peer = P2PBRSClient(
        name=args.name,
        role=args.role,
        server_host=args.server_host,
        server_port=args.server_port,
        udp_port=args.udp_port,
        tcp_port=args.tcp_port,
        storage_capacity=args.storage_capacity,
        base_dir=args.base_dir,
    )
    peer.run()


if __name__ == "__main__":
    main()
