import argparse
import select
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

START, END, DATA, ACK = 0, 1, 2, 3
HEADER_SIZE = 16
MAX_PACKET = 1472
CHUNK_SIZE = MAX_PACKET - HEADER_SIZE  # 1456 bytes dữ liệu / gói
TIMEOUT = 0.5  # 500 ms


def make_packet(pkt_type, seq_num, data=b""):
    header = PacketHeader(type=pkt_type, seq_num=seq_num, length=len(data))
    header.checksum = 0
    header.checksum = compute_checksum(bytes(header) + data)
    return bytes(header) + data


def parse_ack(raw):
    """Trả về seq_num của ACK hợp lệ, hoặc None nếu gói hỏng / không phải ACK."""
    if len(raw) < HEADER_SIZE:
        return None
    header = PacketHeader(raw[:HEADER_SIZE])
    payload = raw[HEADER_SIZE : HEADER_SIZE + header.length]
    received_checksum = header.checksum
    header.checksum = 0
    if compute_checksum(bytes(header) + payload) != received_checksum:
        return None
    if header.type != ACK:
        return None
    return header.seq_num


def wait_for_ack(sock, timeout):
    """Chờ tối đa `timeout` giây; trả về seq_num của ACK hợp lệ hoặc None."""
    ready, _, _ = select.select([sock], [], [], max(timeout, 0))
    if not ready:
        return None
    raw, _ = sock.recvfrom(2048)
    return parse_ack(raw)


def sender(receiver_ip, receiver_port, window_size):
    """Đọc thông điệp từ stdin và gửi tới receiver bằng RTP trên UDP."""
    message = sys.stdin.buffer.read()
    addr = (receiver_ip, receiver_port)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # ---- 1. Thiết lập kết nối: START (seq 0), chờ ACK có seq_num = 1 ----
    start_pkt = make_packet(START, 0)
    while True:
        s.sendto(start_pkt, addr)
        deadline = time.time() + TIMEOUT
        got_ack = False
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            ack = wait_for_ack(s, remaining)
            if ack == 1:
                got_ack = True
                break
        if got_ack:
            break

    # ---- 2. Chia thông điệp thành các gói DATA, seq_num bắt đầu từ 1 ----
    chunks = [message[i : i + CHUNK_SIZE] for i in range(0, len(message), CHUNK_SIZE)]
    total = len(chunks)
    packets = {i + 1: make_packet(DATA, i + 1, chunks[i]) for i in range(total)}
    last_seq = total  # seq_num cuối cùng của DATA
    end_seq = total + 1

    base = 1  # gói cũ nhất chưa được ACK
    next_seq = 1  # gói tiếp theo sẽ gửi lần đầu
    timer_start = None

    while base <= last_seq:
        # Gửi các gói mới trong phạm vi cửa sổ
        while next_seq <= last_seq and next_seq < base + window_size:
            s.sendto(packets[next_seq], addr)
            if timer_start is None:
                timer_start = time.time()
            next_seq += 1

        remaining = TIMEOUT - (time.time() - timer_start)
        if remaining <= 0:
            # Cửa sổ không tiến lên sau 500 ms: truyền lại toàn bộ cửa sổ
            for seq in range(base, next_seq):
                s.sendto(packets[seq], addr)
            timer_start = time.time()
            continue

        ack = wait_for_ack(s, remaining)
        if ack is None:
            continue
        # ACK tích lũy: ack = seq_num của gói tiếp theo receiver mong đợi
        if base < ack <= next_seq:
            base = ack
            timer_start = time.time()  # cửa sổ tiến lên -> reset timer

    # ---- 3. Kết thúc: gửi END, chờ ACK tối đa 500 ms ----
    # Gửi lại END mỗi 100 ms trong cửa sổ 500 ms phòng khi END bị mất
    end_pkt = make_packet(END, end_seq)
    deadline = time.time() + TIMEOUT
    next_send = 0.0
    while True:
        now = time.time()
        if now >= deadline:
            break
        if now >= next_send:
            s.sendto(end_pkt, addr)
            next_send = now + 0.1
        # Với ACK tích lũy, ACK của END trùng seq_num với ACK cuối của DATA nên
        # không phân biệt được ACK cũ/mới: cứ gửi lại END đủ 500 ms rồi mới thoát.
        wait_for_ack(s, min(next_send, deadline) - time.time())

    s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "receiver_ip", help="The IP address of the host that receiver is running on"
    )
    parser.add_argument(
        "receiver_port", type=int, help="The port number on which receiver is listening"
    )
    parser.add_argument(
        "window_size", type=int, help="Maximum number of outstanding packets"
    )
    args = parser.parse_args()

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()