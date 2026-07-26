#!/usr/bin/env python3
"""
Test orchestrator to run `cli.py daemon` and test identify and ping with a local Kubo daemon.
"""
import os
import subprocess
import time
import tempfile
import re
import sys

def start_kubo(ipfs_path: str):
    env = {**os.environ, "IPFS_PATH": ipfs_path}
    for cmd in [
        ["ipfs", "init", "--profile=test"],
        ["ipfs", "config", "--json", "Addresses.Swarm", '["/ip4/127.0.0.1/tcp/0"]'],
        ["ipfs", "bootstrap", "rm", "--all"],
        ["ipfs", "config", "Addresses.API",     "/ip4/127.0.0.1/tcp/0"],
        ["ipfs", "config", "Addresses.Gateway", "/ip4/127.0.0.1/tcp/0"],
    ]:
        subprocess.run(cmd, env=env, check=True, capture_output=True)

    log_path = os.path.join(ipfs_path, "daemon.log")
    proc = subprocess.Popen(["ipfs", "daemon"], env=env, stdout=open(log_path, "w"), stderr=subprocess.STDOUT)
    
    deadline = time.time() + 30
    while time.time() < deadline:
        time.sleep(0.5)
        if proc.poll() is not None:
            raise RuntimeError("Kubo exited unexpectedly.")
        try:
            if "Daemon is ready" in open(log_path).read():
                break
        except FileNotFoundError:
            pass
    else:
        proc.terminate()
        raise RuntimeError("Kubo did not start in 30 s")

    peer_id_str = subprocess.check_output(["ipfs", "id", "-f=<id>"], env=env).decode().strip()
    addrs = subprocess.check_output(["ipfs", "id", "-f=<addrs>"], env=env).decode().strip().splitlines()
    addr = next((a.strip() for a in addrs if "127.0.0.1" in a and "/tcp/" in a), None)
    return proc, peer_id_str, addr

def main():
    print("==============================================================")
    print("  CLI INTEROP TEST: cli.py ↔ Kubo (identify + Kubo pinging us)")
    print("==============================================================")
    with tempfile.TemporaryDirectory(prefix="kubo_cli_") as ipfs_path:
        print("\n[1/4] Starting local Kubo daemon...")
        kubo_proc, kubo_id, kubo_addr = start_kubo(ipfs_path)
        print(f"      ✅ Kubo ready: {kubo_id}")
        print(f"         Address: {kubo_addr}")
        
        cli_log_path = os.path.join(ipfs_path, "cli.log")
        env = {**os.environ, "IPFS_LITE_BOOTSTRAP_PEERS": kubo_addr}
        
        print("\n[2/4] Starting py-ipfs-lite CLI daemon...")
        cli_proc = subprocess.Popen(
            ["uv", "run", "python", "-m", "py_ipfs_lite.cli", "--debug", "daemon", "--port", "0"],
            env=env,
            stdout=open(cli_log_path, "w"),
            stderr=subprocess.STDOUT,
        )
        
        # Wait for CLI to start and connect to Kubo
        deadline = time.time() + 60
        cli_peer_id = None
        while time.time() < deadline:
            time.sleep(0.5)
            if cli_proc.poll() is not None:
                print("❌ CLI process exited early!")
                print(open(cli_log_path).read()[-1000:])
                kubo_proc.terminate()
                sys.exit(1)
            
            try:
                log_content = open(cli_log_path).read()
                if "Successfully joined the DHT network!" in log_content:
                    # Extract CLI Peer ID
                    match = re.search(r"Daemon Peer ID: (12D3Koo[a-zA-Z0-9]+)", log_content)
                    if match:
                        cli_peer_id = match.group(1)
                        break
            except Exception:
                pass
        
        # Extract CLI port from logs to build the multiaddr
        cli_multiaddr = None
        for line in log_content.splitlines():
            # Look for: "/ip4/0.0.0.0/tcp/59250"
            match = re.search(r"/ip4/0\.0\.0\.0/tcp/(\d+)(?!/ws)", line)
            if match:
                cli_multiaddr = f"/ip4/127.0.0.1/tcp/{match.group(1)}"
                break
        
        if not cli_peer_id:
            print("❌ CLI failed to bootstrap within 60s")
            print("--- CLI LOGS ---")
            print(open(cli_log_path).read())
            print("----------------")
            cli_proc.terminate()
            kubo_proc.terminate()
            sys.exit(1)
            
        print(f"      ✅ CLI Daemon is running and bootstrapped to Kubo!")
        print(f"      ✅ CLI Peer ID: {cli_peer_id}")
        if cli_multiaddr:
            print(f"      ✅ CLI Multiaddr: {cli_multiaddr}")
        
        print("\n[3/4] Using Kubo to ping our CLI node 5 times...")
        kubo_env = {**os.environ, "IPFS_PATH": ipfs_path}
        
        if cli_multiaddr:
            connect_res = subprocess.run(["ipfs", "swarm", "connect", f"{cli_multiaddr}/p2p/{cli_peer_id}"], env=kubo_env, capture_output=True, text=True)
            print("      Kubo `ipfs swarm connect` output:", connect_res.stdout.strip())
            if connect_res.stderr:
                print("      Kubo `ipfs swarm connect` stderr:", connect_res.stderr.strip())
            
        ping_res = subprocess.run(["ipfs", "ping", "-n", "5", cli_peer_id], env=kubo_env, capture_output=True, text=True)
        print("      Kubo `ipfs ping` output:")
        for line in ping_res.stdout.strip().splitlines():
            print(f"        {line}")
        if ping_res.stderr:
            print("      Kubo `ipfs ping` stderr:")
            for line in ping_res.stderr.strip().splitlines():
                print(f"        {line}")
            
        if ping_res.returncode == 0 and "Average latency" in ping_res.stdout:
            print("      ✅ Multiple pings successful!")
        else:
            print("      ❌ Ping failed or incomplete output.")
            
        # Stop CLI daemon
        cli_proc.terminate()
        kubo_proc.terminate()
        
        print("\n[4/4] Verifying Identify & Ping traces in CLI logs...")
        cli_logs = open(cli_log_path).read()
        
        has_identify = "/ipfs/id/1.0.0" in cli_logs
        has_ping = "/ipfs/ping/1.0.0" in cli_logs
        
        if has_identify:
            print("      ✅ Found Identify (/ipfs/id/1.0.0) traces in CLI debug logs")
        else:
            print("      ❌ Identify traces missing in CLI debug logs")
            
        if has_ping:
            print("      ✅ Found Ping (/ipfs/ping/1.0.0) traces in CLI debug logs")
        else:
            print("      ❌ Ping traces missing in CLI debug logs")
            
        print("\n==============================================================")
        if has_identify and has_ping and ping_res.returncode == 0:
            print("🎉 CLI INTEROP TEST PASSED: Identify and Pings are fully working!")
        else:
            print("⚠️ TEST FAILED: Some checks did not pass.")
            print("\n--- CLI LOGS TRUNCATED ---")
            print("----------------")
        print("==============================================================")
        
if __name__ == "__main__":
    main()
