
import socket
import threading
import time
import sys

BASE_PORT = 51000
NODES = [1, 2, 3, 4]

HEARTBEAT_INTERVAL = 2
HEARTBEAT_TIMEOUT = 7
ELECTION_TIMEOUT = 2
COORDINATOR_WAIT_TIMEOUT = 4
STARTUP_GRACE = 12


class BullyNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.coordinator = max(NODES)

        self.started_at = time.monotonic()
        self.last_heartbeat = self.started_at
        self.last_coordinator_message = 0.0

        self.lock = threading.Lock()
        self.election_lock = threading.Lock()

        self.is_electing = False
        self.higher_responded = False
        self.has_seen_coordinator = False
        self.election_scheduled = False

        self.sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM
        )
        self.sock.bind(
            ("127.0.0.1", BASE_PORT + node_id)
        )
        self.sock.settimeout(0.5)

        print(
            f"[NODE-{node_id}] Aktif di port "
            f"{BASE_PORT + node_id}. "
            f"Coordinator awal: Node-{self.coordinator}"
        )

    def send_message(self, target_id, message):
        try:
            self.sock.sendto(
                message.encode("utf-8"),
                ("127.0.0.1", BASE_PORT + target_id)
            )
        except OSError as error:
            print(
                f"[ERROR] Gagal mengirim pesan ke "
                f"Node-{target_id}: {error}"
            )

    def broadcast_coordinator(self):
        for target in NODES:
            if target != self.node_id:
                self.send_message(
                    target,
                    f"COORDINATOR:{self.node_id}"
                )

    def declare_coordinator(self):
        with self.lock:
            if self.higher_responded:
                return False

            self.coordinator = self.node_id
            self.last_heartbeat = time.monotonic()
            self.last_coordinator_message = self.last_heartbeat
            self.has_seen_coordinator = True

        print(
            f"\n>>> NODE-{self.node_id} "
            "MENJADI COORDINATOR! <<<\n"
        )

        self.broadcast_coordinator()
        return True

    def start_election(self):
        if not self.election_lock.acquire(blocking=False):
            return

        try:
            with self.lock:
                self.is_electing = True
                self.higher_responded = False

            election_started = time.monotonic()

            print(
                f"\n[ELECTION] Node-{self.node_id} "
                "memulai pemilihan coordinator."
            )

            higher_nodes = [
                node for node in NODES
                if node > self.node_id
            ]

            for target in higher_nodes:
                print(
                    f"[NODE-{self.node_id}] Mengirim "
                    f"ELECTION ke Node-{target}"
                )
                self.send_message(
                    target,
                    f"ELECTION:{self.node_id}"
                )

            deadline = time.monotonic() + ELECTION_TIMEOUT

            while time.monotonic() < deadline:
                with self.lock:
                    responded = self.higher_responded

                if responded:
                    break

                time.sleep(0.05)

            with self.lock:
                responded = self.higher_responded

            if not responded:
                self.declare_coordinator()
                return

            print(
                f"[NODE-{self.node_id}] Node dengan ID "
                "lebih tinggi merespons. Menunggu pengumuman."
            )

            deadline = (
                time.monotonic() + COORDINATOR_WAIT_TIMEOUT
            )

            while time.monotonic() < deadline:
                with self.lock:
                    announcement_received = (
                        self.last_coordinator_message
                        > election_started
                    )

                if announcement_received:
                    break

                time.sleep(0.05)

            with self.lock:
                announcement_received = (
                    self.last_coordinator_message
                    > election_started
                )

            if not announcement_received:
                print(
                    f"[NODE-{self.node_id}] Pengumuman "
                    "coordinator belum diterima."
                )

        finally:
            with self.lock:
                self.is_electing = False

            self.election_lock.release()

    def delayed_election(self, failed_coordinator):
        # Node dengan ID lebih tinggi menunggu lebih singkat.
        delay = (
            max(NODES) - self.node_id
        ) * (ELECTION_TIMEOUT + HEARTBEAT_INTERVAL + 1)

        time.sleep(delay)

        with self.lock:
            should_start = (
                self.coordinator == failed_coordinator
                and self.coordinator != self.node_id
                and time.monotonic() - self.last_heartbeat
                > HEARTBEAT_TIMEOUT
                and not self.is_electing
            )

            self.election_scheduled = False

        if should_start:
            self.start_election()

    def listen_loop(self):
        while True:
            try:
                data, _ = self.sock.recvfrom(1024)

            except socket.timeout:
                continue

            except OSError:
                break

            message = data.decode(
                "utf-8",
                errors="replace"
            ).strip()

            msg_type, separator, sender_text = (
                message.partition(":")
            )

            if not separator or not sender_text.isdigit():
                continue

            sender_id = int(sender_text)

            if (
                sender_id not in NODES
                or sender_id == self.node_id
            ):
                continue

            now = time.monotonic()

            if msg_type == "HEARTBEAT":
                with self.lock:
                    elapsed = now - self.last_heartbeat

                    if (
                        sender_id == self.coordinator
                        or sender_id > self.coordinator
                        or elapsed > HEARTBEAT_TIMEOUT
                    ):
                        self.coordinator = sender_id
                        self.last_heartbeat = now
                        self.last_coordinator_message = now
                        self.has_seen_coordinator = True

                    if sender_id > self.node_id:
                        self.higher_responded = True

            elif msg_type == "ELECTION":
                print(
                    f"[NODE-{self.node_id}] Menerima "
                    f"ELECTION dari Node-{sender_id}; "
                    "mengirim OK."
                )

                self.send_message(
                    sender_id,
                    f"OK:{self.node_id}"
                )

                if self.node_id > sender_id:
                    threading.Thread(
                        target=self.start_election,
                        daemon=True
                    ).start()

            elif msg_type == "OK":
                if sender_id > self.node_id:
                    print(
                        f"[NODE-{self.node_id}] "
                        f"Menerima OK dari Node-{sender_id}."
                    )

                    with self.lock:
                        self.higher_responded = True

            elif msg_type == "COORDINATOR":
                with self.lock:
                    elapsed = now - self.last_heartbeat

                    if (
                        sender_id == self.coordinator
                        or sender_id > self.coordinator
                        or elapsed > HEARTBEAT_TIMEOUT
                    ):
                        self.coordinator = sender_id
                        self.last_heartbeat = now
                        self.last_coordinator_message = now
                        self.has_seen_coordinator = True

                        print(
                            f"[COORDINATOR UPDATE] "
                            f"Node-{self.node_id}: coordinator "
                            f"sekarang Node-{sender_id}"
                        )

                    if sender_id > self.node_id:
                        self.higher_responded = True

    def heartbeat_loop(self):
        while True:
            with self.lock:
                coordinator = self.coordinator

            if coordinator == self.node_id:
                for target in NODES:
                    if target != self.node_id:
                        self.send_message(
                            target,
                            f"HEARTBEAT:{self.node_id}"
                        )

                print(
                    f"[HEARTBEAT] Node-{self.node_id} "
                    "mengirim heartbeat."
                )

            time.sleep(HEARTBEAT_INTERVAL)

    def failure_detector_loop(self):
        while True:
            time.sleep(0.5)
            now = time.monotonic()

            with self.lock:
                coordinator = self.coordinator
                elapsed = now - self.last_heartbeat
                uptime = now - self.started_at

                startup_expired = (
                    self.has_seen_coordinator
                    or uptime > STARTUP_GRACE
                )

                should_schedule = (
                    coordinator != self.node_id
                    and elapsed > HEARTBEAT_TIMEOUT
                    and startup_expired
                    and not self.is_electing
                    and not self.election_scheduled
                )

                if should_schedule:
                    self.election_scheduled = True
                    failed_coordinator = coordinator

            if should_schedule:
                print(
                    f"\n[FAILURE DETECTED] Node-{coordinator} "
                    "tidak mengirim heartbeat."
                )

                threading.Thread(
                    target=self.delayed_election,
                    args=(failed_coordinator,),
                    daemon=True
                ).start()

    def run(self):
        threading.Thread(
            target=self.listen_loop,
            daemon=True
        ).start()

        threading.Thread(
            target=self.heartbeat_loop,
            daemon=True
        ).start()

        threading.Thread(
            target=self.failure_detector_loop,
            daemon=True
        ).start()

        # Node-4 adalah coordinator awal.
        if self.node_id == max(NODES):
            self.declare_coordinator()

        print("Perintah: status | elect | quit")

        try:
            while True:
                command = input().strip().lower()

                if command == "status":
                    with self.lock:
                        print(
                            f"Node-{self.node_id} | "
                            f"Coordinator: Node-{self.coordinator}"
                        )

                elif command == "elect":
                    threading.Thread(
                        target=self.start_election,
                        daemon=True
                    ).start()

                elif command == "quit":
                    break

        except (KeyboardInterrupt, EOFError):
            print(f"\nNode-{self.node_id} dihentikan.")

        finally:
            self.sock.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(
            "Penggunaan: python node_bully_heartbeat.py <ID 1-4>"
        )
        sys.exit(1)

    try:
        node_id = int(sys.argv[1])

        if node_id not in NODES:
            raise ValueError

    except ValueError:
        print("ID node harus berupa angka 1 sampai 4.")
        sys.exit(1)

    node = BullyNode(node_id)
    node.run()
