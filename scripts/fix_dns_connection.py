#!/usr/bin/env python3
"""
Fix DNS issues by using IP address directly
This is a temporary solution - AWS Aurora IPs can change
"""

import os
import socket
import subprocess

# RDS endpoint
RDS_ENDPOINT = "six-worker-cluster.cluster-cg56igkg8ibw.us-east-1.rds.amazonaws.com"

def get_rds_ip():
    """Get current IP for RDS endpoint"""
    try:
        # Try Google DNS
        result = subprocess.run(
            ['nslookup', RDS_ENDPOINT, '8.8.8.8'],
            capture_output=True,
            text=True
        )
        
        # Parse the IP from output
        for line in result.stdout.split('\n'):
            if 'Address:' in line and '#' not in line:
                ip = line.split('Address:')[1].strip()
                if ip and '.' in ip:
                    return ip
        
        # Fallback to socket resolution
        return socket.gethostbyname(RDS_ENDPOINT)
    except Exception as e:
        print(f"Error resolving {RDS_ENDPOINT}: {e}")
        return None

def update_connection_files():
    """Update connection files to use IP instead of hostname"""
    ip = get_rds_ip()
    if not ip:
        print("Could not resolve RDS endpoint to IP")
        return False
    
    print(f"RDS endpoint resolves to: {ip}")
    
    # Files to update
    files_to_update = [
        'examples/propose_api_client.py',
        'scripts/run_sql.sh',
        'scripts/import_iowa_businesses.py'
    ]
    
    for filepath in files_to_update:
        if os.path.exists(filepath):
            print(f"Updating {filepath}...")
            with open(filepath, 'r') as f:
                content = f.read()
            
            # Create backup
            with open(f"{filepath}.backup", 'w') as f:
                f.write(content)
            
            # Replace endpoint with IP
            updated = content.replace(RDS_ENDPOINT, ip)
            
            if updated != content:
                with open(filepath, 'w') as f:
                    f.write(updated)
                print(f"  Updated {filepath} to use IP {ip}")
            else:
                print(f"  No changes needed for {filepath}")
    
    return True

def create_hosts_entry():
    """Create hosts file entry for manual addition"""
    ip = get_rds_ip()
    if ip:
        print(f"\n=== Add to /etc/hosts manually ===")
        print(f"sudo nano /etc/hosts")
        print(f"\n# Add this line:")
        print(f"{ip} {RDS_ENDPOINT}")
        print(f"{ip} six-worker-instance-1.cg56igkg8ibw.us-east-1.rds.amazonaws.com")
        return True
    return False

if __name__ == "__main__":
    print("Fixing DNS resolution issues...")
    
    # Option 1: Show hosts entry for manual addition
    create_hosts_entry()
    
    print("\n=== OR ===\n")
    
    # Option 2: Update files to use IP directly
    print("Update connection files to use IP directly? (y/n): ", end='')
    response = input().strip().lower()
    if response == 'y':
        if update_connection_files():
            print("\nConnection files updated successfully!")
            print("Note: This is temporary - AWS can change IPs")
            print("Restart your import script to continue")