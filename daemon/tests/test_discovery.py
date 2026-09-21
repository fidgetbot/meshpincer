from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from meshpincer.config import Settings
from meshpincer.discovery import discover_serial_device


def settings(tmp_path: Path, **kwargs: object) -> Settings:
    return Settings(
        state_dir=tmp_path,
        socket_path=tmp_path / "meshpincer.sock",
        **kwargs,
    )


def port(
    device: str,
    *,
    vid: int | None,
    pid: int | None,
    serial_number: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        device=device,
        vid=vid,
        pid=pid,
        serial_number=serial_number,
        product="Test companion",
        manufacturer="Test vendor",
    )


def test_discovers_matching_usb_identity(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ports = [
        port("/dev/cu.other", vid=0x1111, pid=0x2222),
        port("/dev/cu.mesh", vid=0x2886, pid=0x1667, serial_number="stable-id"),
    ]
    monkeypatch.setattr("meshpincer.discovery.list_ports.comports", lambda: ports)

    result = discover_serial_device(settings(tmp_path))

    assert result.port == "/dev/cu.mesh"
    assert result.serial_number == "stable-id"


def test_hardware_serial_disambiguates_devices(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    ports = [
        port("/dev/cu.one", vid=0x2886, pid=0x1667, serial_number="one"),
        port("/dev/cu.two", vid=0x2886, pid=0x1667, serial_number="two"),
    ]
    monkeypatch.setattr("meshpincer.discovery.list_ports.comports", lambda: ports)

    result = discover_serial_device(settings(tmp_path, usb_serial="two"))

    assert result.port == "/dev/cu.two"


def test_ambiguous_usb_identity_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    ports = [
        port("/dev/cu.one", vid=0x2886, pid=0x1667),
        port("/dev/cu.two", vid=0x2886, pid=0x1667),
    ]
    monkeypatch.setattr("meshpincer.discovery.list_ports.comports", lambda: ports)

    with pytest.raises(RuntimeError, match="MESHPINCER_USB_SERIAL"):
        discover_serial_device(settings(tmp_path))


def test_explicit_port_bypasses_usb_discovery(tmp_path: Path) -> None:
    result = discover_serial_device(settings(tmp_path, serial_port="/dev/cu.explicit"))

    assert result.port == "/dev/cu.explicit"
