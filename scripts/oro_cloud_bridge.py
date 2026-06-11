#!/usr/bin/env python3
"""
ORo Base - Production Cloud WebSocket Bridge Daemon
--------------------------------------------------
Establishes a persistent secure WebSocket (WSS) connection to the Ogmen Cloud,
handles token generation/rotation, manages presigned media uploads to AWS S3,
and bridges incoming telemetry/control commands to the local C++ Input Layer via ZMQ.
"""

import asyncio
import json
import logging
import mimetypes
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import jwt
import websockets
import zmq
import zmq.asyncio
from math import degrees

# ==========================================
# CONFIGURATION & LOGGING SETUP
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("OroCloudBridge")

CONFIG_PATHS = [
    "/etc/oro/oro_base_edge_layer_config.json",
    "/home/radxa/oro_base/oro_base_edge_layer/config/oro_base_edge_layer_config.json"
]

CAMHEAD_STEP_ANGLE_DEG = 5.0

def load_device_id() -> str:
    """Dynamically loads device_id from global configuration files."""
    for path in CONFIG_PATHS:
        try:
            if os.path.exists(path):
                with open(path, "r") as f:
                    config = json.load(f)
                    dev_id = config.get("global", {}).get("device_id")
                    if dev_id:
                        logger.info(f"Loaded device_id '{dev_id}' from config: {path}")
                        return dev_id
        except Exception as e:
            logger.warning(f"Failed to parse config at {path}: {e}")
    
    fallback = "9e092b69-5973-46e4-a228-fe4933e04364"
    logger.warning(f"Using fallback default device_id: {fallback}")
    return fallback

DEVICE_ID = load_device_id()
USER_ID = os.environ.get("ORO_USER_ID", "ab92596e-3126-4396-8f6e-6ec04bc0ebbe")
JWT_SECRET_KEY = os.environ.get("ORO_JWT_SECRET_KEY", "change-me-in-production-use-a-long-random-string")
JWT_ALGORITHM = "HS256"
SYNC_TOKEN_EXPIRE_MINUTES = 60

BASE_WS_URL = f"wss://orobase.ogmenrobotics.com/ws/devices/{DEVICE_ID}"
CLOUD_SERVER_BASE_URL = "https://orobase.ogmenrobotics.com"

# Local ZMQ Endpoint of C++ Input Layer
ZMQ_CMD_URL = "tcp://127.0.0.1:5555"

# S3 media folders
MEDIA_ROOT_IMAGES = "oro_base_images"
MEDIA_ROOT_VIDEO = "oro_base_video_audio"

# Hardware details (matching C++ ffmpeg capture pipeline)
CAMERA_DEVICE = "/dev/video13"
IMAGE_DIR = "/home/radxa/Pictures/Command_Executor_Images"
VIDEO_DIR = "/home/radxa/Videos/Command_Executor_Videos"

# Ensure target directories exist
os.makedirs(IMAGE_DIR, exist_ok=True)
os.makedirs(VIDEO_DIR, exist_ok=True)


# ==========================================
# TOKEN GENERATION & EXPIRY MANAGER
# ==========================================
class TokenManager:
    def __init__(self):
        self.current_token = None
        self.expires_at = datetime.min.replace(tzinfo=timezone.utc)

    def get_token(self) -> str:
        """Retrieves active token, generating/rotating if expired or near expiry."""
        now = datetime.now(timezone.utc)
        # Rotate token 5 minutes before actual expiry for safety margin
        if not self.current_token or now >= (self.expires_at - timedelta(minutes=5)):
            self.current_token = self._generate_token()
            logger.info("New JWT sync token successfully generated & rotated.")
        return self.current_token

    def _generate_token(self) -> str:
        issued_at = datetime.now(timezone.utc)
        expire = issued_at + timedelta(minutes=SYNC_TOKEN_EXPIRE_MINUTES)
        self.expires_at = expire
        payload = {
            "sub": USER_ID,
            "device_id": DEVICE_ID,
            "scope": "sync",
            "type": "sync",
            "jti": str(uuid.uuid4()),
            "exp": expire,
            "iat": issued_at,
        }
        return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


# ==========================================
# VIDEO RECORDING ROUTINE (FFMPEG COHERENCE)
# ==========================================
def record_video_ffmpeg(duration_sec: int) -> str:
    """
    Records video using ffmpeg from the configured system camera.
    This guarantees hardware resource safety without OpenCV device locks.
    """
    timestamp = int(time.time() * 1000)
    filename = f"ORoBase_VID_{timestamp}.mp4"
    full_path = os.path.join(VIDEO_DIR, filename)

    logger.info(f"Starting hardware video recording on {CAMERA_DEVICE} for {duration_sec}s...")
    # -y (overwrite), -f v4l2 (input format), -t (duration), -c:v (codec)
    cmd = [
        "ffmpeg", "-y", "-f", "v4l2", "-i", CAMERA_DEVICE,
        "-t", str(duration_sec), "-c:v", "libx264", "-pix_fmt", "yuv420p",
        full_path
    ]

    try:
        # Run ffmpeg synchronously in the background executor thread
        res = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=duration_sec + 5)
        if res.returncode == 0:
            logger.info(f"Video recorded and saved successfully: {full_path}")
            return full_path
        else:
            logger.error(f"ffmpeg video recording failed with exit code: {res.returncode}")
    except subprocess.TimeoutExpired:
        logger.error("ffmpeg recording timed out.")
    except Exception as e:
        logger.error(f"Exception during ffmpeg video recording: {e}")
    
    return ""


# ==========================================
# MEDIA STREAMING UPLOADS (S3 FLOW)
# ==========================================
def _iter_file_chunks(file_path: Path, chunk_size: int = 1024 * 1024):
    """Yield file content in 1MB chunks for low-overhead streaming upload."""
    with file_path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            yield chunk

def upload_media_file(sync_token: str, local_filename: str, media_root: str) -> bool:
    """
    Uploads a file to S3 via pre-signed URL transaction (Identical to production edge sync).
    """
    file_path = Path(local_filename)
    if not file_path.exists():
        logger.error(f"File not found, skipping upload: {local_filename}")
        return False

    object_key = f"{media_root}/{file_path.name}"
    content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"

    headers = {"Authorization": f"Bearer {sync_token}"}
    base_url = CLOUD_SERVER_BASE_URL.rstrip("/")
    presign_url = f"{base_url}/sync/{DEVICE_ID}/media/presign"
    status_url = f"{base_url}/sync/{DEVICE_ID}/media/status"

    logger.info(f"Initiating presigned URL flow for: {object_key}")
    with httpx.Client(headers=headers, timeout=60, follow_redirects=True) as client:
        # Step 1: Request pre-signed PUT URL
        try:
            presign_response = client.post(
                presign_url,
                json={"object_key": object_key, "content_type": content_type},
            )
        except Exception as exc:
            logger.error(f"Presign request failed for {object_key}: {exc}")
            return False

        if presign_response.status_code not in {200, 201}:
            logger.error(f"Presign request rejected: {presign_response.status_code} - {presign_response.text}")
            return False

        presign_payload = presign_response.json()
        upload_url = str(presign_payload.get("upload_url") or "").strip()
        if not upload_url:
            logger.error("Presign response missing upload_url")
            return False

        request_headers = {}
        raw_headers = presign_payload.get("headers")
        if isinstance(raw_headers, dict):
            for k, v in raw_headers.items():
                request_headers[str(k)] = str(v)
        request_headers.setdefault("Content-Type", content_type)
        request_headers["Content-Length"] = str(file_path.stat().st_size)

        # Step 2: Stream file directly to AWS S3
        logger.info(f"Streaming file content directly to S3...")
        try:
            upload_response = httpx.put(
                upload_url,
                content=_iter_file_chunks(file_path),
                headers=request_headers,
                timeout=60,
                follow_redirects=True,
            )
        except Exception as exc:
            logger.error(f"S3 stream upload failed: {exc}")
            return False

        if upload_response.status_code not in {200, 201, 204}:
            logger.error(f"S3 upload rejected with status {upload_response.status_code}")
            return False

        storage_url = str(presign_payload.get("storage_url") or "").strip() or None
        if storage_url and storage_url.startswith("s3://"):
            parts = storage_url[5:].split("/", 1)
            if len(parts) == 2:
                bucket, key = parts
                storage_url = f"https://{bucket}.s3.amazonaws.com/{key}"
        if not storage_url and upload_url:
            storage_url = upload_url.split("?")[0]
            
        logger.info(f"S3 Upload Complete. Storage URL: {storage_url}")

        # Step 3: Patch upload status back to Cloud Server
        try:
            status_response = client.patch(
                status_url,
                json={
                    "object_key": object_key,
                    "status": "uploaded",
                    "record_id": None,
                    "s3_url": storage_url,
                    "error": None,
                },
            )
            if status_response.status_code not in {200, 201}:
                logger.error(f"Failed to report upload status: HTTP {status_response.status_code} - {status_response.text}")
                return False
        except Exception as exc:
            logger.error(f"Status report transaction failed: {exc}")
            return False

    logger.info(f"Successfully synced S3 object state for {object_key}")
    return True


# ==========================================
# ASYNC DAEMON ORCHESTRATOR
# ==========================================
class CloudBridgeDaemon:
    def __init__(self):
        self.token_manager = TokenManager()
        self.zmq_ctx = zmq.asyncio.Context()
        self.zmq_sock = None
        self.running = False
        
        
        # Camhead rotation tracking
        self.camhead_last_time = None
        self.camhead_last_speed = 0.0
        self.camhead_last_direction = 1
        self.camhead_accumulated_angle_rad = 0.0

    def reset_zmq_socket(self):
        """Recreates ZMQ REQ socket to reset the EFSM in case of request errors."""
        if self.zmq_sock:
            try:
                self.zmq_sock.close(linger=0)
            except Exception:
                pass
        self.zmq_sock = self.zmq_ctx.socket(zmq.REQ)
        self.zmq_sock.setsockopt(zmq.LINGER, 0)
        self.zmq_sock.connect(ZMQ_CMD_URL)
        logger.info(f"ZMQ command socket connected to C++ Input Ingress: {ZMQ_CMD_URL}")

    async def send_zmq_command(self, cmd_dict: dict, timeout: float = 21.0) -> dict:
        """Sends command JSON over ZMQ loopback with strict timeouts and FSM safety."""
        cmd_str = json.dumps(cmd_dict)
        try:
            await self.zmq_sock.send_string(cmd_str)
            reply = await asyncio.wait_for(self.zmq_sock.recv_string(), timeout=timeout)
            return json.loads(reply)
        except asyncio.CancelledError:
            logger.warning("ZMQ transaction cancelled. Resetting socket to clear state...")
            self.reset_zmq_socket()
            raise
        except (asyncio.TimeoutError, Exception) as e:
            logger.error(f"ZMQ Command Transaction Error: {type(e).__name__} - {e}")
            self.reset_zmq_socket()  # Clear state FSM
            return {"status": "error", "message": f"Edge core communications failure: {str(e)}"}

    def process_camhead_rotation(self, payload: dict):
        """
        Every movement command becomes a fixed-angle step.

        direction = 1   -> clockwise 5°
        direction = -1  -> counter-clockwise 5°
        direction = 0   -> stop
        """

        action = str(payload.get("action", "move")).lower()
        direction = int(payload.get("direction", 0))

        # Stop command
        if direction == 0:
            logger.info("CAMHEAD STOP received")
            return action, None, None

        logger.info(
            f"CAMHEAD STEP | direction={direction} | "
            f"angle={CAMHEAD_STEP_ANGLE_DEG} deg"
        )

        return action, CAMHEAD_STEP_ANGLE_DEG, direction

    # def process_camhead_rotation(self, payload: dict):
    #     """
    #     Integrate incoming angular velocity commands and estimate
    #     total angle rotated until a stop command is received.
    #     """
    #     now = time.monotonic()

    #     action = str(payload.get("action", "move")).lower()
    #     direction = int(payload.get("direction", 0))
    #     speed = float(payload.get("speed", 0.0))

    #     calculated_angle_deg = None
    #     calculated_direction = None

    #     if self.camhead_last_time is not None:
    #         dt = now - self.camhead_last_time
    #         delta_angle = self.camhead_last_speed * dt

    #         if self.camhead_last_direction == 1:
    #             self.camhead_accumulated_angle_rad += delta_angle
    #         else:
    #             self.camhead_accumulated_angle_rad -= delta_angle

    #     self.camhead_last_time = now
    #     self.camhead_last_speed = speed
    #     self.camhead_last_direction = direction

    #     if speed > 0:
    #         logger.info(
    #             "CAMHEAD MOVE | "
    #             f"dir={direction} | "
    #             f"speed={speed:.3f} rad/s | "
    #             f"angle={degrees(abs(self.camhead_accumulated_angle_rad)):.2f} deg"
    #         )

    #     if speed == 0.0 and abs(self.camhead_accumulated_angle_rad) > 0:
    #         calculated_angle_deg = degrees(abs(self.camhead_accumulated_angle_rad))
    #         calculated_direction = 1 if self.camhead_accumulated_angle_rad >= 0 else -1

    #         direction_str = (
    #             "CLOCKWISE"
    #             if self.camhead_accumulated_angle_rad > 0
    #             else "COUNTER_CLOCKWISE"
    #         )

    #         logger.info(
    #             "\n"
    #             "=====================================================\n"
    #             "CAMHEAD ROTATION COMPLETE\n"
    #             f"Direction : {direction_str}\n"
    #             f"Angle     : {abs(self.camhead_accumulated_angle_rad):.6f} rad\n"
    #             f"Angle     : {calculated_angle_deg:.2f} deg\n"
    #             "====================================================="
    #         )

    #         self.camhead_accumulated_angle_rad = 0.0
    #         self.camhead_last_time = None
    #         self.camhead_last_speed = 0.0

    #     return action,calculated_angle_deg, calculated_direction

    async def handle_websocket_message(self, websocket, message_str: str):
        """Processes incoming messages from the Cloud WSS connection."""
        try:
            data = json.loads(message_str)
            
            # Skip server error or event frames to prevent ping-pong feedback loops
            if data.get("type") == "error" or "detail" in data:
                # logger.warning(f"Skipping server status/error event to avoid feedback loop: {data}")
                return

            logger.info(f"Incoming cloud WSS request: {data}")
            
            command_type = data.get("command_type")
            payload = data.get("payload", {})
            command_id = data.get("command_id", f"ws_{int(time.time())}")

            sync_token = self.token_manager.get_token()

            if command_type == "photo_capture":
                # Route photo capture to C++ edge handler via ZMQ
                logger.info("Routing photo capture request to C++...")
                reply = await self.send_zmq_command({"topic": "/commands/photo_capture", "value": 1.0})
                
                if reply.get("status") == "success" and "storage_path" in reply:
                    local_path = reply["storage_path"]
                    # Offload blocking network I/O upload flow to threadpool
                    logger.info(f"Triggering asynchronous S3 upload for: {local_path}")
                    success = await asyncio.get_event_loop().run_in_executor(
                        None, upload_media_file, sync_token, local_path, MEDIA_ROOT_IMAGES
                    )
                    
                    ws_rep = {
                        "command_id": command_id,
                        "status": "success" if success else "failed",
                        "operation": "photo_capture",
                        "uploaded": success,
                        "file_id": reply.get("file_id")
                    }
                else:
                    ws_rep = {
                        "command_id": command_id,
                        "status": "failed",
                        "error": reply.get("error", "camera_capture_failed")
                    }
                await websocket.send(json.dumps(ws_rep))

            elif command_type == "record_video":
                duration = int(payload.get("duration", 10))
                logger.info(f"Routing video capture command to C++ command executor (duration={duration})...")
                # Construct structured command containing signal_id to route directly to CommandExecutor
                cmd_dict = {
                    "signal_id": 135,
                    "signal_type": "video_capture_command_event",
                    "command_id": command_id,
                    "issued_by": "cloud_user",
                    "event_time": int(time.time() * 1000),
                    "payload": {
                        "duration": duration
                    }
                }
                reply = await self.send_zmq_command(cmd_dict)
                
                # C++ handler responds instantly on success, initiating background capture.
                if reply.get("status") == "success":
                    ws_rep = {
                        "command_id": command_id,
                        "status": "success",
                        "operation": "record_video",
                        "message": "video_capture_initiated"
                    }
                else:
                    ws_rep = {
                        "command_id": command_id,
                        "status": "failed",
                        "error": reply.get("message", "video_capture_initiation_failed")
                    }
                await websocket.send(json.dumps(ws_rep))

            elif command_type == "start_record_video":
                logger.info("Routing start continuous video command to C++...")
                cmd_dict = {
                    "signal_id": 135,
                    "signal_type": "video_capture_command_event",
                    "command_id": command_id,
                    "issued_by": "cloud_user",
                    "event_time": int(time.time() * 1000),
                    "payload": {
                        "action": "start"
                    }
                }
                reply = await self.send_zmq_command(cmd_dict)
                if reply.get("status") == "success":
                    ws_rep = {
                        "command_id": command_id,
                        "status": "success",
                        "operation": "start_record_video",
                        "message": "continuous_recording_started"
                    }
                else:
                    ws_rep = {
                        "command_id": command_id,
                        "status": "failed",
                        "error": reply.get("message", "failed_to_start_video")
                    }
                await websocket.send(json.dumps(ws_rep))

            elif command_type == "stop_record_video":
                logger.info("Routing stop continuous video command to C++...")
                cmd_dict = {
                    "signal_id": 135,
                    "signal_type": "video_capture_command_event",
                    "command_id": command_id,
                    "issued_by": "cloud_user",
                    "event_time": int(time.time() * 1000),
                    "payload": {
                        "action": "stop"
                    }
                }
                reply = await self.send_zmq_command(cmd_dict)
                if reply.get("status") == "success":
                    ws_rep = {
                        "command_id": command_id,
                        "status": "success",
                        "operation": "stop_record_video",
                        "message": "continuous_recording_stopped"
                    }
                else:
                    ws_rep = {
                        "command_id": command_id,
                        "status": "failed",
                        "error": reply.get("message", "failed_to_stop_video")
                    }
                await websocket.send(json.dumps(ws_rep))

            elif command_type == "record_video_pan":
                logger.info("Routing panoramic video record command to C++...")
                cmd_dict = {
                    "signal_id": 139,
                    "signal_type": "record_video_pan_command_event",
                    "command_id": command_id,
                    "issued_by": "cloud_user",
                    "event_time": int(time.time() * 1000),
                    "payload": {}
                }
                reply = await self.send_zmq_command(cmd_dict)
                if reply.get("status") == "success":
                    ws_rep = {
                        "command_id": command_id,
                        "status": "success",
                        "operation": "record_video_pan",
                        "message": "pan_video_capture_initiated"
                    }
                else:
                    ws_rep = {
                        "command_id": command_id,
                        "status": "failed",
                        "error": reply.get("message", "failed_to_initiate_pan")
                    }
                await websocket.send(json.dumps(ws_rep))

            elif command_type == "camhead_message":
                action = str(payload.get("action", "")).lower()
                if action == "home":
                    logger.info("Routing homing command to C++...")
                    reply = await self.send_zmq_command({
                        "topic": "/commands/camera_rotation",
                        "payload": {
                            "action": "home"
                        }
                    })
                    reply["command_id"] = command_id
                    await websocket.send(json.dumps(reply))
                else:
                    action_returned, angle_deg, direction = self.process_camhead_rotation(payload)
                    if angle_deg is not None and direction is not None:
                        logger.info(f"Calculated camhead rotation: angle={angle_deg:.2f} deg, direction={direction}")
                        reply = await self.send_zmq_command({
                            "topic": "/commands/camera_rotation",
                            "payload": {
                                "action": action_returned,
                                "angle": angle_deg,
                                "direction": direction
                            }
                        })
                        reply["command_id"] = command_id
                        await websocket.send(json.dumps(reply))
                    else:
                        # Intermediate movement commands just get success back
                        ws_rep = {
                            "command_id": command_id,
                            "status": "success",
                            "operation": "camhead_message",
                            "message": "move_registered"
                        }
                        await websocket.send(json.dumps(ws_rep))

                    
            elif command_type == "treat_dispense":
                qty = float(payload.get("quantity", 1.0))
                speed = int(payload.get("speed", 3))
                logger.info(f"Routing treat dispense request to C++ (qty={qty}, speed={speed})...")
                reply = await self.send_zmq_command({
                    "topic": "/commands/treat/dispense",
                    "value": qty,
                    "speed": speed
                })
                reply["command_id"] = command_id
                await websocket.send(json.dumps(reply))

            elif command_type == "play_music":
                track = str(payload.get("music_full_name", "breaking_bad.mp3"))
                logger.info(f"Routing play music request to C++ (track={track})...")
                action_code = 1
                if "breaking_bad" in track:
                    action_code = 1
                elif "dandelions" in track:
                    action_code = 2
                elif "call_this_love" in track:
                    action_code = 3
                
                file_id = track.rsplit('.', 1)[0] if '.' in track else track
                storage_path = f"/home/radxa/Music/{track}"
                
                reply = await self.send_zmq_command({
                    "signal_id": 137,
                    "signal_type": "play_music_event",
                    "command_id": command_id,
                    "issued_by": "cloud_user",
                    "event_time": int(time.time() * 1000),
                    "payload": {
                        "action_code": action_code,
                        "file_id": file_id,
                        "storage_path": storage_path,
                        "event_time": int(time.time() * 1000)
                    }
                })
                reply["command_id"] = command_id
                await websocket.send(json.dumps(reply))

            elif command_type == "stop_music":
                logger.info("Routing stop music request to C++...")
                reply = await self.send_zmq_command({
                    "signal_id": 138,
                    "signal_type": "stop_music_event",
                    "command_id": command_id,
                    "issued_by": "cloud_user",
                    "event_time": int(time.time() * 1000),
                    "payload": {
                        "action_code": 0,
                        "file_id": "stop",
                        "storage_path": "",
                        "event_time": int(time.time() * 1000)
                    }
                })
                reply["command_id"] = command_id
                await websocket.send(json.dumps(reply))

            elif command_type == "food_bowl_control":
                bowl = 1 if str(payload.get("bowl")).lower() == "left" else 2
                action = 0 if str(payload.get("action")).lower() == "close" else 1
                if bowl is not None and action is not None:   # ← safe check
                    logger.info(f"Routing feed request to C++ ({bowl} | {action})...")
                    reply = await self.send_zmq_command({
                        "topic": "/commands/feed",
                        "lid_id": bowl,
                        "action": action
                    })
                    reply["command_id"] = 64
                    await websocket.send(json.dumps(reply))
                    logger.info(f"Food bowl control result: {reply}")
                else:
                    logger.warning(f"Invalid food bowl command: {payload}")

            else:
                # Generic pass-through mapping
                zmq_topic = payload.get("topic")
                zmq_val = float(payload.get("value", 0.0))
                
                if zmq_topic:
                    logger.info(f"Routing generic command topic '{zmq_topic}' to C++...")
                    reply = await self.send_zmq_command({
                        "topic": zmq_topic,
                        "value": zmq_val,
                        **payload
                    })
                    reply["command_id"] = command_id
                    await websocket.send(json.dumps(reply))
                else:
                    logger.warning(f"Unmapped websocket command: {command_type}")

        except Exception as e:
            logger.error(f"Failed to handle WSS incoming message: {e}")

    async def run(self):
        self.running = True
        self.reset_zmq_socket()
        
        backoff_sec = 1
        max_backoff_sec = 60

        while self.running:
            try:
                sync_token = self.token_manager.get_token()
                ws_url = f"{BASE_WS_URL}?token={sync_token}"
                
                logger.info(f"Connecting to Cloud secure WSS endpoint: {BASE_WS_URL}")
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                    logger.info("⚡ WSS Link successfully established with ogmen cloud backend!")
                    backoff_sec = 1 # reset backoff on success
                    
                    async for message in ws:
                        # Process messages asynchronously without blocking reading loop
                        asyncio.create_task(self.handle_websocket_message(ws, message))

            except (websockets.ConnectionClosed, Exception) as e:
                if not self.running:
                    break
                logger.error(f"WebSocket link connection error: {e}")
                logger.info(f"Attempting reconnection in {backoff_sec}s...")
                await asyncio.sleep(backoff_sec)
                backoff_sec = min(max_backoff_sec, backoff_sec * 2)

    def stop(self):
        self.running = False
        if self.zmq_sock:
            self.zmq_sock.close(linger=0)
        self.zmq_ctx.term()
        logger.info("Daemon cleanly shut down.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ORo Base Cloud WebSocket Bridge & CLI Uploader")
    parser.add_argument("--upload", help="Path to local file to upload directly to S3")
    parser.add_argument("--type", choices=["image", "video"], default="video", help="Media type (default: video)")
    args = parser.parse_args()

    if args.upload:
        logger.info(f"CLI Mode: Uploading file {args.upload} (type={args.type})...")
        token_mgr = TokenManager()
        tok = token_mgr.get_token()
        media_root = MEDIA_ROOT_VIDEO if args.type == "video" else MEDIA_ROOT_IMAGES
        ok = upload_media_file(tok, args.upload, media_root)
        sys.exit(0 if ok else 1)

    logger.info("Initializing ORO Base Cloud WebSocket Bridge Daemon...")
    daemon = CloudBridgeDaemon()
    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        logger.info("SIGINT received, exiting gracefully...")
        daemon.stop()
