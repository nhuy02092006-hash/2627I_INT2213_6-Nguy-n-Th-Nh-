import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum

START, END, DATA, ACK = 0, 1, 2, 3
HEADER_SIZE = 16


def make_ack(seq_num):
    header = PacketHeader(type=ACK, seq_num=seq_num, length=0)
    header.checksum = 0
    header.checksum = compute_checksum(bytes(header))
    return bytes(header)


def receiver(receiver_ip, receiver_port, window_size):
    """Lắng nghe trên socket UDP và ghi thông điệp nhận được ra stdout."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))
    out = sys.stdout.buffer

    connected = False
    expected = 0  # seq_num của gói DATA tiếp theo cần nhận (N)
    buffer = {}  # gói đến không theo thứ tự: seq_num -> payload

    while True:
        raw, address = s.recvfrom(2048)
        if len(raw) < HEADER_SIZE:
            continue

        header = PacketHeader(raw[:HEADER_SIZE])
        payload = raw[HEADER_SIZE : HEADER_SIZE + header.length]
        if len(payload) != header.length:
            continue

        # Kiểm tra checksum: sai thì bỏ gói, không gửi ACK
        received_checksum = header.checksum
        header.checksum = 0
        if compute_checksum(bytes(header) + payload) != received_checksum:
            continue

        pkt_type = header.type
        seq = header.seq_num

        if pkt_type == START:
            # Bỏ qua START nếu đang ở giữa một kết nối
            if connected:
                continue
            connected = True
            expected = 1
            buffer = {}
            s.sendto(make_ack(1), address)

        elif pkt_type == DATA:
            if not connected:
                continue
            # Ngoài cửa sổ [N, N + window_size): loại bỏ
            if seq >= expected + window_size:
                continue

            if seq == expected:
                out.write(payload)
                expected += 1
                # Đẩy ra các gói đã đệm liên tiếp
                while expected in buffer:
                    out.write(buffer.pop(expected))
                    expected += 1
            elif seq > expected:
                buffer[seq] = payload
            # seq < expected: gói trùng lặp, chỉ cần gửi lại ACK

            out.flush()  # ghi xuống ngay phòng khi tiến trình bị dừng
            # ACK tích lũy: seq_num của gói tiếp theo mong đợi
            s.sendto(make_ack(expected), address)

        elif pkt_type == END:
            if not connected:
                continue
            s.sendto(make_ack(seq), address)
            out.flush()
            break

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

    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()