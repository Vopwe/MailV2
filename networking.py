"""
Network helpers for search IP rotation setup and validation.
"""
from __future__ import annotations

import ipaddress
import json
import os
import platform
import shlex
import subprocess
from typing import Callable

import config


Runner = Callable[..., subprocess.CompletedProcess]


def normalize_ip_list(value) -> list[str]:
    items: list[str] = []
    if isinstance(value, list):
        source = value
    else:
        raw = str(value or "")
        source = raw.replace(",", "\n").splitlines()

    seen: set[str] = set()
    for item in source:
        text = str(item or "").strip()
        if not text:
            continue
        try:
            parsed = ipaddress.ip_address(text)
        except ValueError:
            continue
        normalized = parsed.compressed
        if normalized not in seen:
            seen.add(normalized)
            items.append(normalized)
    return items


def _run_command(command: list[str], runner: Runner | None = None) -> subprocess.CompletedProcess:
    runner = runner or subprocess.run
    return runner(command, capture_output=True, text=True, check=False)


def detect_default_interface(runner: Runner | None = None) -> str | None:
    if runner is None and platform.system().lower() != "linux":
        return None

    for command in (["ip", "route", "show", "default"], ["ip", "-6", "route", "show", "default"]):
        result = _run_command(command, runner=runner)
        if result.returncode != 0:
            continue
        for line in result.stdout.splitlines():
            parts = line.split()
            if "dev" in parts:
                index = parts.index("dev")
                if index + 1 < len(parts):
                    return parts[index + 1]
    return None


def detect_local_ips(interface: str | None = None, runner: Runner | None = None) -> dict:
    requested_interface = (interface or "").strip() or detect_default_interface(runner=runner)
    if runner is None and platform.system().lower() != "linux":
        return {
            "supported": False,
            "interface": requested_interface,
            "requested_interface": interface,
            "ipv4": [],
            "ipv6": [],
            "assigned_ips": [],
            "error": "Local IP detection is only supported on Linux servers.",
        }

    command = ["ip", "-j", "addr", "show"]
    if requested_interface:
        command.extend(["dev", requested_interface])

    result = _run_command(command, runner=runner)
    if result.returncode != 0:
        return {
            "supported": False,
            "interface": requested_interface,
            "requested_interface": interface,
            "ipv4": [],
            "ipv6": [],
            "assigned_ips": [],
            "error": (result.stderr or result.stdout or "Unable to inspect local interfaces.").strip(),
        }

    try:
        payload = json.loads(result.stdout or "[]")
    except json.JSONDecodeError:
        payload = []

    ipv4: list[str] = []
    ipv6: list[str] = []
    ipv4_prefixlen: int | None = None
    ipv6_prefixlen: int | None = None
    interface_name = requested_interface

    for iface in payload:
        if interface_name is None:
            interface_name = iface.get("ifname")
        for addr in iface.get("addr_info", []):
            if addr.get("scope") != "global":
                continue
            local = str(addr.get("local") or "").strip()
            if not local:
                continue
            try:
                parsed = ipaddress.ip_address(local)
            except ValueError:
                continue
            if parsed.version == 6:
                ipv6.append(parsed.compressed)
                if ipv6_prefixlen is None:
                    ipv6_prefixlen = int(addr.get("prefixlen") or 64)
            else:
                ipv4.append(parsed.compressed)
                if ipv4_prefixlen is None:
                    ipv4_prefixlen = int(addr.get("prefixlen") or 24)

    assigned = normalize_ip_list(ipv4 + ipv6)
    return {
        "supported": True,
        "interface": interface_name,
        "requested_interface": interface,
        "ipv4": normalize_ip_list(ipv4),
        "ipv6": normalize_ip_list(ipv6),
        "ipv4_prefixlen": ipv4_prefixlen or 24,
        "ipv6_prefixlen": ipv6_prefixlen or 64,
        "assigned_ips": assigned,
        "error": None,
    }


def build_netplan_snippet(
    interface: str,
    desired_ips: list[str],
    *,
    ipv4_prefixlen: int = 24,
    ipv6_prefixlen: int = 64,
) -> str:
    interface = (interface or "eth0").strip() or "eth0"
    addresses = []
    for ip in normalize_ip_list(desired_ips):
        suffix = f"/{ipv6_prefixlen}" if ipaddress.ip_address(ip).version == 6 else f"/{ipv4_prefixlen}"
        addresses.append(f"      - {ip}{suffix}")

    body = "\n".join(addresses) if addresses else "      []"
    return (
        "network:\n"
        "  version: 2\n"
        "  ethernets:\n"
        f"    {interface}:\n"
        "      addresses:\n"
        f"{body}\n"
    )


def build_rotation_plan(
    *,
    interface: str | None = None,
    candidate_ips=None,
    configured_ips=None,
    runner: Runner | None = None,
) -> dict:
    local = detect_local_ips(interface=interface, runner=runner)
    assigned_set = set(local["assigned_ips"])
    configured_list = normalize_ip_list(configured_ips)
    candidate_list = normalize_ip_list(candidate_ips)
    effective_candidate_list = candidate_list or configured_list

    candidate_assigned = [ip for ip in effective_candidate_list if ip in assigned_set]
    candidate_missing = [ip for ip in effective_candidate_list if ip not in assigned_set]
    configured_assigned = [ip for ip in configured_list if ip in assigned_set]
    configured_missing = [ip for ip in configured_list if ip not in assigned_set]

    desired_for_netplan = normalize_ip_list(local["assigned_ips"] + effective_candidate_list)
    recommended_outbound = candidate_assigned or configured_assigned or local["assigned_ips"]

    return {
        "interface": local["interface"] or (interface or "eth0"),
        "supported": local["supported"],
        "error": local["error"],
        "assigned_ipv4": local["ipv4"],
        "assigned_ipv6": local["ipv6"],
        "ipv4_prefixlen": local.get("ipv4_prefixlen", 24),
        "ipv6_prefixlen": local.get("ipv6_prefixlen", 64),
        "assigned_ips": local["assigned_ips"],
        "candidate_ips": effective_candidate_list,
        "saved_candidate_ips": candidate_list,
        "candidate_assigned_ips": candidate_assigned,
        "candidate_missing_ips": candidate_missing,
        "configured_ips": configured_list,
        "configured_assigned_ips": configured_assigned,
        "configured_missing_ips": configured_missing,
        "recommended_outbound_ips": recommended_outbound,
        "netplan_snippet": build_netplan_snippet(
            local["interface"] or (interface or "eth0"),
            desired_for_netplan,
            ipv4_prefixlen=local.get("ipv4_prefixlen", 24),
            ipv6_prefixlen=local.get("ipv6_prefixlen", 64),
        ),
        "desired_netplan_ips": desired_for_netplan,
        "configure_command": build_configure_command(local["interface"] or (interface or "eth0")),
    }


def build_configure_command(interface: str | None = None) -> str:
    base_dir = config.BASE_DIR.replace("\\", "/")
    parts = [
        "sudo",
        "python3",
        f"{base_dir}/deploy/configure_ip_pool.py",
    ]
    if interface:
        parts.extend(["--interface", interface])
    parts.extend(["--apply", "--enable-rotation"])
    return " ".join(shlex.quote(part) for part in parts)


def get_saved_rotation_candidates() -> list[str]:
    return normalize_ip_list(config.get_setting("rotation_candidate_ips", []))


def get_saved_rotation_interface() -> str:
    return str(config.get_setting("rotation_network_interface", "") or "").strip()
