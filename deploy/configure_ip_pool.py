#!/usr/bin/env python3
"""
Configure a permanent GraphenMail search IP pool on Linux.

Run as root on the server:
    sudo python3 /opt/graphenmail/deploy/configure_ip_pool.py --apply --enable-rotation
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import config
import networking


DEFAULT_NETPLAN_PATH = "/etc/netplan/60-graphenmail-ip-pool.yaml"


def _read_ips_file(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _interactive_ip_input() -> str:
    print("Paste one IP per line. Submit an empty line to finish.")
    lines: list[str] = []
    while True:
        try:
            line = input("> ").strip()
        except EOFError:
            break
        if not line:
            break
        lines.append(line)
    return "\n".join(lines)


def _write_netplan(path: str, snippet: str):
    Path(path).write_text(snippet, encoding="utf-8")


def _apply_netplan():
    subprocess.run(["netplan", "generate"], check=True)
    subprocess.run(["netplan", "apply"], check=True)


def _update_settings(interface: str, candidate_ips: list[str], working_ips: list[str], enable_rotation: bool):
    config.save_settings({
        "rotation_network_interface": interface,
        "rotation_candidate_ips": candidate_ips,
        "outbound_ips": working_ips,
        "search_ip_rotation_enabled": bool(enable_rotation and working_ips),
        "sync_outbound_ips_from_candidates": True,
    })


def main() -> int:
    parser = argparse.ArgumentParser(description="Configure a permanent GraphenMail IP rotation pool.")
    parser.add_argument("--interface", default="", help="Network interface to manage, for example eth0")
    parser.add_argument("--ips", default="", help="Comma-separated extra IPs to add")
    parser.add_argument("--ips-file", default="", help="Read extra IPs from a file")
    parser.add_argument("--netplan-path", default=DEFAULT_NETPLAN_PATH, help="Netplan file path to write")
    parser.add_argument("--apply", action="store_true", help="Apply the generated netplan file immediately")
    parser.add_argument("--enable-rotation", action="store_true", help="Enable search rotation after verification")
    parser.add_argument("--interactive", action="store_true", help="Prompt for missing values interactively")
    args = parser.parse_args()

    if os.geteuid() != 0:
        print("Run this helper as root.", file=sys.stderr)
        return 1

    interface = (args.interface or config.get_setting("rotation_network_interface", "") or networking.detect_default_interface() or "eth0").strip()

    raw_ips = ""
    if args.ips:
        raw_ips = args.ips
    elif args.ips_file:
        raw_ips = _read_ips_file(args.ips_file)
    else:
        saved = config.get_setting("rotation_candidate_ips", [])
        raw_ips = "\n".join(saved) if isinstance(saved, list) else str(saved or "")

    if args.interactive and not raw_ips.strip():
        raw_ips = _interactive_ip_input()

    candidate_ips = networking.normalize_ip_list(raw_ips)
    if not candidate_ips:
        print("No candidate IPs supplied. Save provider IPs in Settings first, or pass --ips / --ips-file.", file=sys.stderr)
        return 1

    pre_plan = networking.build_rotation_plan(interface=interface, candidate_ips=candidate_ips)
    print(f"Interface: {pre_plan['interface']}")
    print(f"Currently assigned IPs: {', '.join(pre_plan['assigned_ips']) or 'none'}")
    print(f"Candidate IPs: {', '.join(candidate_ips)}")
    print(f"Missing candidate IPs before apply: {', '.join(pre_plan['candidate_missing_ips']) or 'none'}")

    _write_netplan(args.netplan_path, pre_plan["netplan_snippet"])
    print(f"Wrote {args.netplan_path}")

    if args.apply:
        _apply_netplan()
        print("Applied netplan.")

    post_plan = networking.build_rotation_plan(interface=interface, candidate_ips=candidate_ips)
    working_ips = post_plan["candidate_assigned_ips"]
    missing_ips = post_plan["candidate_missing_ips"]

    _update_settings(post_plan["interface"], candidate_ips, working_ips, args.enable_rotation)

    print(f"Working IPs after apply: {', '.join(working_ips) or 'none'}")
    if missing_ips:
        print(f"Still missing from interface: {', '.join(missing_ips)}")
    print(f"GraphenMail outbound_ips synced to {len(working_ips)} working IPs.")
    if args.enable_rotation:
        print("Search rotation enabled." if working_ips else "Search rotation left disabled because no working IPs were found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
