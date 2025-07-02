#!/usr/bin/env python3
"""
WebSocket Agent - Runs on the target machine behind NAT
Connects to Node.js relay server via WebSocket and executes commands
"""

import asyncio
import websockets
import json
import subprocess
import os
import uuid
import sys
from datetime import datetime

class WebSocketAgent:
    def __init__(self, relay_host, relay_port=3000, agent_id=None):
        self.relay_host = relay_host
        self.relay_port = relay_port
        self.agent_id = agent_id or str(uuid.uuid4())[:8]
        self.current_dir = os.getcwd()
        self.running = True
        self.websocket = None
        
    def log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] [Agent {self.agent_id}] {message}")
        
    def execute_command(self, command):
        """Execute a command and return the result"""
        try:
            if command.strip().startswith('cd '):
                # Handle cd command specially
                path = command.strip()[3:].strip()
                if not path:
                    path = os.path.expanduser('~')
                
                if os.path.isabs(path):
                    new_dir = path
                else:
                    new_dir = os.path.join(self.current_dir, path)
                
                new_dir = os.path.abspath(new_dir)
                
                if os.path.exists(new_dir) and os.path.isdir(new_dir):
                    self.current_dir = new_dir
                    # Don't use os.chdir() as it affects the entire process
                    # Instead, just update our tracking variable
                    return f"Changed directory to: {self.current_dir}"
                else:
                    return f"Directory not found: {new_dir}"
            else:
                # Execute other commands in the current directory
                # For Windows, use a more robust approach that handles cross-drive access
                if os.name == 'nt':  # Windows
                    # Use cmd /c with explicit directory change to handle cross-drive access
                    # The /d flag allows changing both directory and drive
                    cmd_to_execute = f'cmd /c "cd /d "{self.current_dir}" && {command}"'
                else:
                    # Non-Windows systems
                    cmd_to_execute = command
                
                result = subprocess.run(
                    cmd_to_execute,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                output = result.stdout
                if result.stderr:
                    output += f"\nSTDERR: {result.stderr}"
                    
                return output or f"Command executed (exit code: {result.returncode})"
                
        except subprocess.TimeoutExpired:
            return "Command timed out (30 seconds)"
        except Exception as e:
            return f"Error executing command: {str(e)}"
    
    async def send_message(self, message):
        """Send message to relay server"""
        if self.websocket:
            try:
                # Check if websocket is open using the correct method
                if hasattr(self.websocket, 'closed'):
                    is_open = not self.websocket.closed
                elif hasattr(self.websocket, 'open'):
                    is_open = self.websocket.open
                else:
                    # For websockets library, we can try to send and catch exceptions
                    is_open = True
                
                if is_open:
                    await self.websocket.send(json.dumps(message))
                else:
                    self.log("WebSocket is not open, cannot send message")
            except Exception as e:
                self.log(f"Error sending message: {e}")
        else:
            self.log("No WebSocket connection available")
    
    async def handle_message(self, message_data):
        """Handle incoming messages from relay server"""
        try:
            message = json.loads(message_data)
            message_type = message.get('type')
            
            if message_type == 'welcome':
                self.log("Connected to relay server")
                
            elif message_type == 'register_ack':
                if message.get('status') == 'success':
                    self.log("Successfully registered with relay server")
                else:
                    self.log("Failed to register with relay server")
                    
            elif message_type == 'command':
                command = message.get('command')
                command_id = message.get('command_id')
                
                self.log(f"Received command: {command} (ID: {command_id})")
                
                # Execute command
                result = self.execute_command(command)
                
                self.log(f"Command execution completed. Result length: {len(result)}")
                
                # Send response back
                response = {
                    'type': 'command_response',
                    'command_id': command_id,
                    'agent_id': self.agent_id,
                    'command': command,
                    'result': result,
                    'current_dir': self.current_dir,
                    'timestamp': datetime.now().isoformat()
                }
                
                self.log(f"Sending response for command {command_id}")
                await self.send_message(response)
                self.log("Response sent successfully")
                
            elif message_type == 'error':
                self.log(f"Error from server: {message.get('message')}")
                
            elif message_type == 'pong':
                # Response to ping - keep connection alive
                pass
                
        except Exception as e:
            self.log(f"Error handling message: {e}")
    
    async def connect_to_relay(self):
        """Connect to the relay server via WebSocket"""
        uri = f"ws://{self.relay_host}:{self.relay_port}"
        
        while self.running:
            try:
                self.log(f"Connecting to relay server at {uri}")
                
                async with websockets.connect(uri) as websocket:
                    self.websocket = websocket
                    
                    # Register with relay server
                    register_msg = {
                        'type': 'agent_register',
                        'agent_id': self.agent_id
                    }
                    
                    await self.send_message(register_msg)
                    
                    # Handle incoming messages
                    async for message in websocket:
                        await self.handle_message(message)
                        
            except websockets.exceptions.ConnectionClosed:
                self.log("Connection closed by server")
            except Exception as e:
                self.log(f"Connection error: {e}")
                
            if self.running:
                self.log("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
    
    async def send_periodic_ping(self):
        """Send periodic ping to keep connection alive"""
        while self.running:
            try:
                if self.websocket:
                    # Check if websocket is still open
                    is_open = False
                    if hasattr(self.websocket, 'closed'):
                        is_open = not self.websocket.closed
                    elif hasattr(self.websocket, 'open'):
                        is_open = self.websocket.open
                    
                    if is_open:
                        await self.send_message({'type': 'ping'})
                await asyncio.sleep(30)  # Ping every 30 seconds
            except:
                pass
    
    async def start(self):
        """Start the agent"""
        self.log(f"Agent starting with ID: {self.agent_id}")
        self.log(f"Current directory: {self.current_dir}")
        
        try:
            # Run connection and ping tasks concurrently
            await asyncio.gather(
                self.connect_to_relay(),
                self.send_periodic_ping()
            )
        except KeyboardInterrupt:
            self.log("Agent shutting down...")
            self.running = False

def main():
    if len(sys.argv) < 2:
        print("Usage: python websocket_agent.py <relay_server_ip> [relay_port] [agent_id]")
        sys.exit(1)
        
    relay_host = sys.argv[1]
    relay_port = int(sys.argv[2]) if len(sys.argv) > 2 else 3000
    agent_id = sys.argv[3] if len(sys.argv) > 3 else None
    
    agent = WebSocketAgent(relay_host, relay_port, agent_id)
    
    # Install websockets if not available
    try:
        import websockets
    except ImportError:
        print("Please install websockets: pip install websockets")
        sys.exit(1)
    
    # Run the agent
    asyncio.run(agent.start())

if __name__ == "__main__":
    main()