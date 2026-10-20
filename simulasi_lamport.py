import time
import queue


class LamportNode:
    def __init__(self, node_id, total_nodes):
        self.node_id = node_id
        self.total_nodes = total_nodes
        self.clock = 0
        self.inbox = queue.Queue()

    def log(self, action):
        waktu = time.strftime('%H:%M:%S')
        print(
            f"[{waktu}] Node-{self.node_id} "
            f"(Clock: {self.clock:02d}) -> {action}"
        )

    def internal_event(self):
        self.clock += 1
        self.log("Eksekusi INTERNAL EVENT")

    def send_message(self, target_node, message_text):
        self.clock += 1
        timestamp_send = self.clock

        payload = {
            'sender': self.node_id,
            'timestamp': timestamp_send,
            'content': message_text
        }

        self.log(
            f"SEND ke Node-{target_node.node_id}: "
            f"'{message_text}' "
            f"[Timestamp Send: {timestamp_send}]"
        )

        target_node.inbox.put(payload)

    def process_inbox(self):
        while not self.inbox.empty():
            msg = self.inbox.get()

            sender = msg['sender']
            t_msg = msg['timestamp']
            content = msg['content']

            # Aturan Lamport:
            # max(clock lokal, timestamp message) + 1
            clock_before = self.clock
            self.clock = max(self.clock, t_msg) + 1

            self.log(
                f"RECEIVE dari Node-{sender}: "
                f"'{content}' "
                f"[Timestamp Msg: {t_msg} | "
                f"Clock: {clock_before} -> {self.clock}]"
            )


def run_lamport_simulation():

    print("=== SIMULASI LAMPORT LOGICAL CLOCK ===\n")

    node1 = LamportNode(node_id=1, total_nodes=3)
    node2 = LamportNode(node_id=2, total_nodes=3)
    node3 = LamportNode(node_id=3, total_nodes=3)

    # Rangkaian Event Skenario

    node1.internal_event()

    node2.internal_event()

    # Node 1 kirim message ke Node 2
    node1.send_message(
        node2,
        "Halo Node 2 dari Node 1"
    )

    node2.process_inbox()

    # Node 2 eksekusi internal event
    # lalu forward message ke Node 3
    node2.internal_event()

    node2.send_message(
        node3,
        "Update data dari Node 2 ke Node 3"
    )

    # Node 3 melakukan internal event
    # sebelum memproses inbox
    node3.internal_event()
    node3.internal_event()

    node3.process_inbox()

    print("\n=== FINAL LOGICAL CLOCK STATE ===")

    print(
        f"Node-1 Clock: {node1.clock} | "
        f"Node-2 Clock: {node2.clock} | "
        f"Node-3 Clock: {node3.clock}"
    )


if __name__ == '__main__':
    run_lamport_simulation()