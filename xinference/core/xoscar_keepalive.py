"""
Monkey-patch xoscar SocketClient.connect to enable TCP keepalive on every
RPC connection.

Without this, idle TCP connections between Supervisor and Worker are silently
dropped by stateful network devices (firewalls, NAT gateways) whose TCP
session timeout (commonly 15-45 min) is shorter than the application idle
window.  xoscar's ``Router._cache`` only checks ``writer.is_closing()``
which stays ``False`` after a silent drop, so the dead connection is reused
and the next RPC hangs until OS-level TCP retransmission timeout (60-120 s).

Enabling ``SO_KEEPALIVE`` with ``TCP_KEEPIDLE=60 / TCP_KEEPINTVL=10 /
TCP_KEEPCNT=3`` causes the kernel to probe idle connections every 60 s and
declare them dead within 90 s -- well below typical firewall timeouts.
"""

import logging
import socket

logger = logging.getLogger(__name__)

_patched = False


def patch_xoscar_socket_keepalive():
    """Apply the keepalive patch exactly once (idempotent)."""
    global _patched
    if _patched:
        return
    _patched = True

    try:
        from xoscar.backends.communication.socket import SocketClient
    except ImportError:
        logger.debug("xoscar not installed, skipping keepalive patch")
        return

    _original_connect = SocketClient.connect

    @staticmethod
    async def _patched_connect(dest_address, local_address=None, **kwargs):
        client = await _original_connect(
            dest_address, local_address=local_address, **kwargs
        )
        try:
            sock = client.channel.writer.get_extra_info("socket")
            if sock is not None:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                if hasattr(socket, "TCP_KEEPIDLE"):
                    # Linux
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
                elif hasattr(socket, "TCP_KEEPALIVE"):
                    # macOS: TCP_KEEPALIVE is the equivalent of TCP_KEEPIDLE
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, 60)
        except Exception:
            logger.debug(
                "Failed to set TCP keepalive on xoscar connection to %s",
                dest_address,
                exc_info=True,
            )
        return client

    SocketClient.connect = _patched_connect
    logger.info("xoscar SocketClient.connect patched with TCP keepalive")


# Auto-apply on import
patch_xoscar_socket_keepalive()
