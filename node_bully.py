import socket
import threading
import time
import sys


BASE_PORT = 5000


class BullyNode:

    def __init__(self, node_id, all_nodes_list):

        self.node_id = node_id
        self.all_nodes = sorted(all_nodes_list)
        self.port = BASE_PORT + self.node_id

        self.coordinator = max(self.all_nodes)

        self.is_electing = False
        self.higher_responded = False

        self.sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM
        )

        self.sock.bind(
            ('127.0.0.1', self.port)
        )

        print(
            f"[NODE-{self.node_id}] "
            f"Listening pada Port {self.port}. "
            f"Current Coordinator: Node-{self.coordinator}"
        )

    def send_to_node(self, target_id, message):

        target_port = BASE_PORT + target_id

        try:
            self.sock.sendto(
                message.encode('utf-8'),
                ('127.0.0.1', target_port)
            )
        except Exception:
            pass

    def start_election(self):

        if self.is_electing:
            return

        self.is_electing = True
        self.higher_responded = False

        print(
            f"\n[ELECTION INITIATED] "
            f"Node-{self.node_id} "
            f"memulai proses Leader Election!"
        )

        higher_nodes = [
            nid for nid in self.all_nodes
            if nid > self.node_id
        ]

        if not higher_nodes:

            self.declare_coordinator()

            self.is_electing = False

            return

        for target in higher_nodes:

            print(
                f"[NODE-{self.node_id}] "
                f"Mengirim message 'ELECTION' "
                f"ke Node-{target}"
            )

            self.send_to_node(
                target,
                f"ELECTION:{self.node_id}"
            )

        time.sleep(2)

        if not self.higher_responded:

            self.declare_coordinator()

        self.is_electing = False

    def declare_coordinator(self):

        self.coordinator = self.node_id

        print(
            f"\n>>> [ELECTION WON] "
            f"Node-{self.node_id} "
            f"mendeklarasikan diri sebagai "
            f"NEW COORDINATOR! <<<"
        )

        for target in self.all_nodes:

            if target != self.node_id:

                self.send_to_node(
                    target,
                    f"COORDINATOR:{self.node_id}"
                )

    def listen_loop(self):

        while True:

            data, _ = self.sock.recvfrom(1024)

            message = data.decode('utf-8').strip()

            msg_type, _, sender_str = message.partition(':')

            sender_id = (
                int(sender_str)
                if sender_str
                else 0
            )

            if msg_type == "ELECTION":

                print(
                    f"[NODE-{self.node_id}] "
                    f"Menerima ELECTION dari "
                    f"Node-{sender_id}. "
                    f"Mengirim 'OK'."
                )

                self.send_to_node(
                    sender_id,
                    f"OK:{self.node_id}"
                )

                threading.Thread(
                    target=self.start_election,
                    daemon=True
                ).start()

            elif msg_type == "OK":

                print(
                    f"[NODE-{self.node_id}] "
                    f"Menerima OK dari "
                    f"Node-{sender_id}. "
                    f"Menunggu deklarasi Coordinator."
                )

                self.higher_responded = True

            elif msg_type == "COORDINATOR":

                self.coordinator = sender_id

                print(
                    f"[COORDINATOR UPDATE] "
                    f"Coordinator aktif saat ini: "
                    f"Node-{self.coordinator}"
                )

                self.is_electing = False


def start_node(node_id):

    nodes_cluster = [1, 2, 3, 4]

    node = BullyNode(
        node_id,
        nodes_cluster
    )

    listener = threading.Thread(
        target=node.listen_loop,
        daemon=True
    )

    listener.start()

    print(
        "Ketik 'elect' untuk memicu "
        "Leader Election, atau 'status' "
        "untuk cek Coordinator:"
    )

    while True:

        cmd = input()

        if cmd.lower() == 'elect':

            node.start_election()

        elif cmd.lower() == 'status':

            print(
                f"Current Coordinator: "
                f"Node-{node.coordinator}"
            )


if __name__ == '__main__':

    if len(sys.argv) < 2:

        print(
            "Usage: python node_bully.py "
            "<NODE_ID (1-4)>"
        )

        sys.exit(1)

    start_node(
        int(sys.argv[1])
    )