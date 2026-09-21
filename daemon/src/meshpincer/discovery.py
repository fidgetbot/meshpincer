from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from serial.tools import list_ports

from .config import Settings


class PortInfo(Protocol):
    device: str
    vid: int | None
    pid: int | None
    serial_number: str | None
    product: str | None
    manufacturer: str | None


@dataclass(frozen=True, slots=True)
class SerialDevice:
    port: str
    vid: int | None = None
    pid: int | None = None
    serial_number: str | None = None
    product: str | None = None
    manufacturer: str | None = None


def discover_serial_device(settings: Settings) -> SerialDevice:
    if settings.serial_port:
        return SerialDevice(port=settings.serial_port)

    matches: list[PortInfo] = []
    for port in list_ports.comports():
        if port.vid != settings.usb_vid or port.pid != settings.usb_pid:
            continue
        if settings.usb_serial and port.serial_number != settings.usb_serial:
            continue
        matches.append(port)

    identity = f"{settings.usb_vid:04x}:{settings.usb_pid:04x}"
    if settings.usb_serial:
        identity += " with the configured hardware serial"
    if not matches:
        raise FileNotFoundError(f"no MeshCore companion matching USB {identity}")
    if len(matches) > 1:
        raise RuntimeError(
            f"multiple MeshCore companions match USB {identity}; set MESHPINCER_USB_SERIAL"
        )

    match = matches[0]
    return SerialDevice(
        port=match.device,
        vid=match.vid,
        pid=match.pid,
        serial_number=match.serial_number,
        product=match.product,
        manufacturer=match.manufacturer,
    )
