#!/usr/bin/env python3
import sys
import os
import time
import signal
import argparse
import zmq
import cv2
import numpy as np
from pathlib import Path

# Add media common path to import generated flatbuffers
media_path = Path("/home/radxa/oro_base/oro_base_unit_test_codes/media")
sys.path.insert(0, str(media_path))
sys.path.insert(0, str(media_path / "generated"))

try:
    from media_common import parse_video_frame, nv12_to_bgr, ENDPOINT_VIDEO
except ImportError as e:
    print(f"Import error: {e}")
    sys.exit(1)

running = True

def handle_sig(signum, frame):
    global running
    print(f"Received signal {signum}, stopping recording...")
    running = False

signal.signal(signal.SIGINT, handle_sig)
signal.signal(signal.SIGTERM, handle_sig)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, help="Output file path")
    parser.add_argument("--endpoint", default=ENDPOINT_VIDEO, help="ZMQ Endpoint")
    parser.add_argument("--fps", type=float, default=30.0, help="Target FPS")
    args = parser.parse_args()

    # Ensure output directory exists
    out_dir = os.path.dirname(args.output)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    # Setup ZMQ subscriber
    ctx = zmq.Context()
    sock = ctx.socket(zmq.SUB)
    sock.setsockopt(zmq.RCVTIMEO, 1000)
    sock.connect(args.endpoint)
    sock.setsockopt(zmq.SUBSCRIBE, b"")

    writer = None
    print(f"Subscribing to {args.endpoint} to record to {args.output}")

    try:
        while running:
            try:
                raw = sock.recv()
            except zmq.Again:
                continue
            
            frame = parse_video_frame(raw)
            if frame is None:
                continue

            w, h = frame.Width(), frame.Height()
            nv12 = bytes(frame.DataAsNumpy())
            try:
                bgr = nv12_to_bgr(nv12, w, h)
            except Exception as e:
                print(f"Error converting NV12: {e}")
                continue

            if writer is None:
                # Initialize VideoWriter on first valid frame using mp4v codec
                fourcc = cv2.VideoWriter_fourcc(*'mp4v')
                writer = cv2.VideoWriter(args.output, fourcc, args.fps, (w, h))
                print(f"VideoWriter initialized: {w}x{h} @ {args.fps} FPS")

            writer.write(bgr)

    finally:
        if writer is not None:
            writer.release()
            print("VideoWriter released.")
        sock.close()
        ctx.term()
        print("ZMQ context terminated.")

if __name__ == "__main__":
    main()
