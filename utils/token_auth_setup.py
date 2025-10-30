#!/usr/bin/env python3
"""
Token-based Databricks Authentication Setup Script

This script:
1. Generates a fresh OAuth token using Databricks CLI
2. Extracts the token from Databricks config
3. Updates the .env file with the new token
4. Provides a simple token-based client for queries

Usage:
    python token_auth_setup.py [--refresh-token] [--test-connection]
"""

import os
import json
import subprocess
import sys
from pathlib import Path
from typing import Optional, Dict
import argparse
import configparser


class DatabricksTokenSetup:
    """Simplified Databricks setup using CLI-generated tokens."""
    
    def __init__(self, workspace_root: Optional[Path] = None):
        """Initialize with workspace root directory."""
        self.workspace_root = workspace_root or Path.cwd()
        self.env_file = self.workspace_root / '.env'
        self.venv_python = self.workspace_root / '.venv' / 'bin' / 'python'
        self.venv_databricks = self.workspace_root / '.venv' / 'bin' / 'databricks'
        
        # Databricks connection details
        self.host = "https://mcafee-mosaic-databricks-etl01-dev.cloud.databricks.com"
        self.hostname = "mcafee-mosaic-databricks-etl01-dev.cloud.databricks.com"
        self.http_path = "/sql/1.0/warehouses/0468b6bc765c667c"
    
    def check_databricks_cli(self) -> bool:
        """Check if Databricks CLI is available in the virtual environment."""
        print(f"🔍 Checking Databricks CLI availability...")
        print(f"   📁 Workspace root: {self.workspace_root}")
        print(f"   🐍 Expected venv python: {self.venv_python}")
        print(f"   🛠️  Expected venv databricks: {self.venv_databricks}")
        print(f"   ✅ Venv python exists: {self.venv_python.exists()}")
        print(f"   ✅ Venv databricks exists: {self.venv_databricks.exists()}")
        
        if not self.venv_databricks.exists():
            print("❌ Databricks CLI not found in virtual environment")
            print(f"   Expected location: {self.venv_databricks}")
            print("   Install with: pip install databricks-cli")
            return False
        print("✅ Databricks CLI found in virtual environment")
        return True
    
    def generate_oauth_token(self) -> bool:
        """Generate OAuth token using Databricks CLI."""
        print("🔑 Generating OAuth token using Databricks CLI...")
        print(f"   🔧 Virtual env databricks CLI: {self.venv_databricks}")
        print(f"   🔧 Virtual env exists: {self.venv_databricks.exists()}")
        print(f"   🔧 Host: {self.host}")
        
        try:
            # Run databricks configure with OAuth
            cmd = [
                str(self.venv_databricks),
                "configure",
                "--oauth",
                "--host", self.host
            ]
            
            print(f"   🚀 Running: {' '.join(cmd)}")
            print("   📝 This will open a browser window for OAuth authentication")
            print("   ⏳ Waiting for OAuth completion...")
            
            result = subprocess.run(
                cmd,
                capture_output=False,  # Let user interact with browser
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            print(f"   📊 OAuth process completed with return code: {result.returncode}")
            
            if result.returncode == 0:
                print("✅ OAuth configuration completed successfully")
                return True
            else:
                print(f"❌ OAuth configuration failed with return code: {result.returncode}")
                return False
                
        except subprocess.TimeoutExpired:
            print("❌ OAuth configuration timed out (5 minutes)")
            return False
        except Exception as e:
            print(f"❌ Error during OAuth configuration: {e}")
            import traceback
            print(f"   🔍 Traceback: {traceback.format_exc()}")
            return False
    
    def extract_token_from_config(self) -> Optional[str]:
        """Extract access token from Databricks CLI configuration."""
        print("🔍 Extracting token from Databricks configuration...")
        
        # Check home directory and config file location
        databricks_cfg_path = Path.home() / '.databrickscfg'
        print(f"   📁 Home directory: {Path.home()}")
        print(f"   📄 Config file path: {databricks_cfg_path}")
        print(f"   📄 Config file exists: {databricks_cfg_path.exists()}")
      
        # Fallback: try to read from config file
        try:
            if databricks_cfg_path.exists():
                print(f"   📖 Reading config file: {databricks_cfg_path}")
                config = configparser.ConfigParser()
                config.read(databricks_cfg_path)
                
                print(f"   📋 Found sections: {list(config.sections())}")
                
                # Look for the profile matching our host
                for section_name in config.sections():
                    section = config[section_name]
                    section_host = section.get('host')
                    print(f"   🔍 Section '{section_name}': host='{section_host}'")
                    
                    # Normalize hosts by removing trailing slashes for comparison
                    normalized_section_host = section_host.rstrip('/') if section_host else None
                    normalized_target_host = self.host.rstrip('/')
                    
                    print(f"   🔍 Normalized section host: '{normalized_section_host}'")
                    print(f"   🔍 Normalized target host: '{normalized_target_host}'")
                    
                    if normalized_section_host == normalized_target_host:
                        token = section.get('token')
                        print(f"   🎯 Found matching section: {section_name}")
                        print(f"   🔑 Token present: {bool(token)}")
                        if token:
                            print(f"   🔑 Token length: {len(token)} characters")
                            print(f"   🔑 Token preview: {token[:20]}...")
                            return token
                        else:
                            print(f"   ❌ No token found in matching section")
                            
                print("   ⚠️  No matching host found in any section")
            else:
                print("   ❌ Config file does not exist")
                            
        except Exception as e:
            print(f"⚠️  Could not read from .databrickscfg: {e}")
            import traceback
            print(f"   🔍 Traceback: {traceback.format_exc()}")
        
        print("❌ Could not extract access token")
        return None
    
    def update_env_file(self, access_token: str) -> bool:
        """Update .env file with the new access token."""
        print("📝 Updating .env file with new token...")
        print(f"   📄 Target file: {self.env_file}")
        print(f"   🔑 Token length: {len(access_token)} characters")
        
        try:
            # Read existing .env content
            env_content = {}
            if self.env_file.exists():
                print("   📖 Reading existing .env file...")
                with open(self.env_file, 'r') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            key, value = line.split('=', 1)
                            env_content[key] = value
                            print(f"     Line {line_num}: {key}=<value>")
                print(f"   📋 Found {len(env_content)} existing environment variables")
            else:
                print("   📄 Creating new .env file...")
            
            # Update with new values
            new_values = {
                'DATABRICKS_HOST': self.host,
                'DATABRICKS_SERVER_HOSTNAME': self.hostname,
                'DATABRICKS_HTTP_PATH': self.http_path,
                'DATABRICKS_ACCESS_TOKEN': access_token,
                'DATABRICKS_AUTH_TYPE': 'token'  # Changed from oauth-u2m to token
            }
            
            print("   🔄 Updating with new values:")
            for key, value in new_values.items():
                if key == 'DATABRICKS_ACCESS_TOKEN':
                    print(f"     {key}=<token_{len(value)}_chars>")
                else:
                    print(f"     {key}={value}")
            
            env_content.update(new_values)
            
            # Write back to .env file
            print(f"   💾 Writing {len(env_content)} variables to .env file...")
            with open(self.env_file, 'w') as f:
                for key, value in env_content.items():
                    f.write(f"{key}={value}\n")
            
            print(f"✅ Updated {self.env_file}")
            return True
            
        except Exception as e:
            print(f"❌ Error updating .env file: {e}")
            import traceback
            print(f"   🔍 Traceback: {traceback.format_exc()}")
            return False
    
    def test_connection(self) -> bool:
        """Test the connection using the simple query client."""
        print("🧪 Testing connection with new token...")
        
        try:
            # Import and test the simple query client
            from databricks_query import DatabricksQueryClient
            
            client = DatabricksQueryClient(env_path=self.env_file, debug=True)
            
            # Simple test query
            test_query = "SELECT 1 as test_value"
            result = client.execute_query(test_query)
            
            if result is not None and len(result) > 0:
                print("✅ Connection test successful!")
                print(f"   Test result: {result.iloc[0, 0]}")
                return True
            else:
                print("❌ Connection test failed - no results returned")
                return False
                
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False
    
    def setup_token_auth(self, refresh_token: bool = False) -> bool:
        """Complete token authentication setup process."""
        print("🚀 Setting up Databricks token-based authentication")
        print(f"   📁 Workspace: {self.workspace_root}")
        print(f"   🌐 Host: {self.host}")
        print(f"   📄 Env file: {self.env_file}")
        print(f"   📄 Env file exists: {self.env_file.exists()}")
        print(f"   🔄 Refresh token requested: {refresh_token}")
        print()
        
        # Check prerequisites
        print("1️⃣  Checking prerequisites...")
        if not self.check_databricks_cli():
            return False
        
        # Generate token if needed
        should_generate = refresh_token or not self.env_file.exists()
        print(f"2️⃣  Token generation needed: {should_generate}")
        
        if should_generate:
            print("   🔄 Generating new OAuth token...")
            if not self.generate_oauth_token():
                print("   ❌ Failed to generate OAuth token")
                return False
        else:
            print("   ⏭️  Skipping token generation (env file exists and refresh not requested)")
        
        # Extract token
        print("3️⃣  Extracting token from configuration...")
        access_token = self.extract_token_from_config()
        if not access_token:
            print("   ❌ Failed to extract access token")
            return False
        
        # Update .env file
        print("4️⃣  Updating .env file...")
        if not self.update_env_file(access_token):
            print("   ❌ Failed to update .env file")
            return False
        
        print()
        print("✅ Token-based authentication setup complete!")
        print("   You can now use the simplified DatabricksQueryClient for all queries")
        return True


def main():
    """Main CLI interface."""
    parser = argparse.ArgumentParser(description="Setup Databricks token-based authentication")
    parser.add_argument('--refresh-token', action='store_true', 
                       help='Force refresh of OAuth token')
    parser.add_argument('--test-connection', action='store_true',
                       help='Test connection after setup')
    parser.add_argument('--workspace', type=Path,
                       help='Workspace root directory (default: current directory)')
    
    args = parser.parse_args()
    
    setup = DatabricksTokenSetup(workspace_root=args.workspace)
    
    success = setup.setup_token_auth(refresh_token=args.refresh_token)
    
    if success and args.test_connection:
        setup.test_connection()
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
