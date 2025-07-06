# Enhanced Distributed Chat System
# Features: Multi-client/multi-server, UDP, Bully algorithm, Fault tolerance

import socket
import threading
import pickle
import time
import uuid
import enum
import struct
import sys
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
UNICODE = 'utf-8'
SERVER_PORT = 10000
MULTICAST_GROUP_IP = '224.3.29.71'
MULTICAST_PORT = 10001
HEARTBEAT_INTERVAL = 3.0
HEARTBEAT_TIMEOUT = 9.0
ELECTION_TIMEOUT = 5.0

@dataclass
class ServerInfo:
    """Server information container"""
    uuid: str
    ip: str
    port: int
    last_heartbeat: datetime
    
    def __lt__(self, other):
        return self.uuid < other.uuid

@dataclass
class ClientInfo:
    """Client information container"""
    name: str
    address: Tuple[str, int]
    join_time: datetime

class MessageType(enum.Enum):
    """Message types for system communication"""
    # Client messages
    CLIENT_JOIN = 'CLIENT_JOIN'
    CLIENT_CHAT = 'CLIENT_CHAT'
    CLIENT_QUIT = 'CLIENT_QUIT'
    
    # Server discovery
    SERVER_DISCOVERY = 'SERVER_DISCOVERY'
    SERVER_ANNOUNCE = 'SERVER_ANNOUNCE'
    
    # Leader election (Bully algorithm)
    ELECTION = 'ELECTION'
    OK = 'OK'
    COORDINATOR = 'COORDINATOR'
    
    # Heartbeat
    HEARTBEAT = 'HEARTBEAT'
    HEARTBEAT_ACK = 'HEARTBEAT_ACK'
    
    # Fault detection
    SERVER_CRASH = 'SERVER_CRASH'
    BYZANTINE_ALERT = 'BYZANTINE_ALERT'

@dataclass
class Message:
    """Standardized message format"""
    msg_type: MessageType
    sender_id: str
    data: dict
    timestamp: datetime
    sequence_num: int = 0

class FaultDetector:
    """Handles fault detection and recovery"""
    
    def __init__(self, server_id: str):
        self.server_id = server_id
        self.suspected_servers = set()
        self.byzantine_evidence = {}
        
    def detect_crash_fault(self, servers: Dict[str, ServerInfo]) -> List[str]:
        """Detect crashed servers based on heartbeat timeout"""
        crashed = []
        now = datetime.now()
        
        for server_id, info in servers.items():
            if server_id != self.server_id:
                if now - info.last_heartbeat > timedelta(seconds=HEARTBEAT_TIMEOUT):
                    crashed.append(server_id)
                    logger.warning(f"Detected crash fault: {server_id}")
        
        return crashed
    
    def detect_byzantine_fault(self, message: Message, expected_behavior: dict) -> bool:
        """Detect Byzantine faults through behavior analysis"""
        sender = message.sender_id
        
        # Check for inconsistent messages
        if sender not in self.byzantine_evidence:
            self.byzantine_evidence[sender] = []
        
        # Add evidence collection logic here
        # For now, simple duplicate message detection
        evidence = self.byzantine_evidence[sender]
        for prev_msg in evidence:
            if (prev_msg['type'] == message.msg_type and 
                prev_msg['timestamp'] == message.timestamp and
                prev_msg['data'] != message.data):
                logger.warning(f"Byzantine behavior detected from {sender}")
                return True
        
        evidence.append({
            'type': message.msg_type,
            'timestamp': message.timestamp,
            'data': message.data
        })
        
        # Keep only recent evidence
        if len(evidence) > 10:
            evidence.pop(0)
        
        return False

class BullyElection:
    """Implements Bully algorithm for leader election"""
    
    def __init__(self, server_id: str, servers: Dict[str, ServerInfo]):
        self.server_id = server_id
        self.servers = servers
        self.election_in_progress = False
        self.coordinator = None
        self.election_timeout = ELECTION_TIMEOUT
        
    def start_election(self, socket_ref) -> bool:
        """Start leader election using Bully algorithm"""
        if self.election_in_progress:
            return False
            
        logger.info(f"Starting election from {self.server_id}")
        self.election_in_progress = True
        
        # Send ELECTION message to all servers with higher UUIDs
        higher_servers = [s for s in self.servers.values() 
                         if s.uuid > self.server_id and s.uuid != self.server_id]
        
        if not higher_servers:
            # No higher servers, become coordinator
            self.become_coordinator(socket_ref)
            return True
        
        # Send election messages
        election_msg = Message(
            msg_type=MessageType.ELECTION,
            sender_id=self.server_id,
            data={},
            timestamp=datetime.now()
        )
        
        responses = 0
        for server in higher_servers:
            try:
                socket_ref.sendto(
                    pickle.dumps(election_msg),
                    (server.ip, server.port)
                )
                responses += 1
            except Exception as e:
                logger.error(f"Failed to send election message to {server.uuid}: {e}")
        
        # Wait for responses
        threading.Timer(self.election_timeout, self.handle_election_timeout).start()
        return True
    
    def handle_election_message(self, message: Message, socket_ref, sender_addr):
        """Handle incoming election message"""
        # Send OK response
        ok_msg = Message(
            msg_type=MessageType.OK,
            sender_id=self.server_id,
            data={},
            timestamp=datetime.now()
        )
        
        try:
            socket_ref.sendto(pickle.dumps(ok_msg), sender_addr)
        except Exception as e:
            logger.error(f"Failed to send OK response: {e}")
        
        # Start own election if not already in progress
        if not self.election_in_progress:
            self.start_election(socket_ref)
    
    def become_coordinator(self, socket_ref):
        """Become the coordinator and announce to all servers"""
        self.coordinator = self.server_id
        self.election_in_progress = False
        
        logger.info(f"Became coordinator: {self.server_id}")
        
        # Send COORDINATOR message to all other servers
        coordinator_msg = Message(
            msg_type=MessageType.COORDINATOR,
            sender_id=self.server_id,
            data={'coordinator': self.server_id},
            timestamp=datetime.now()
        )
        
        for server in self.servers.values():
            if server.uuid != self.server_id:
                try:
                    socket_ref.sendto(
                        pickle.dumps(coordinator_msg),
                        (server.ip, server.port)
                    )
                except Exception as e:
                    logger.error(f"Failed to send coordinator message: {e}")
    
    def handle_election_timeout(self):
        """Handle election timeout - become coordinator if no higher server responded"""
        if self.election_in_progress:
            self.election_in_progress = False
            # Become coordinator since no higher server responded
            # This would be called from the server's socket context

class DistributedChatServer:
    """Main server class with fault tolerance and leader election"""
    
    def __init__(self, port: int = SERVER_PORT):
        self.server_id = str(uuid.uuid4())
        self.port = port
        self.running = False
        
        # Network components
        self.udp_socket = None
        self.multicast_socket = None
        
        # State management
        self.servers: Dict[str, ServerInfo] = {}
        self.clients: Dict[str, ClientInfo] = {}
        self.is_leader = False
        self.leader_id = None
        
        # Fault tolerance
        self.fault_detector = FaultDetector(self.server_id)
        self.bully_election = None
        
        # Threading
        self.threads = []
        self.shutdown_event = threading.Event()
        
        # Message sequencing
        self.message_sequence = 0
        
        logger.info(f"Server initialized with ID: {self.server_id}")
    
    def start(self):
        """Start the server"""
        try:
            self.running = True
            self._setup_sockets()
            self._initialize_election()
            self._start_threads()
            
            logger.info(f"Server started on port {self.port}")
            
            # Discover existing servers
            self._discover_servers()
            
            # Start election if we're the first server
            if len(self.servers) == 1:  # Only ourselves
                self.bully_election.become_coordinator(self.udp_socket)
                self.is_leader = True
                self.leader_id = self.server_id
            
            self._run_main_loop()
            
        except Exception as e:
            logger.error(f"Server startup failed: {e}")
            self.stop()
    
    def stop(self):
        """Stop the server gracefully"""
        logger.info("Stopping server...")
        self.running = False
        self.shutdown_event.set()
        
        # Close sockets
        if self.udp_socket:
            self.udp_socket.close()
        if self.multicast_socket:
            self.multicast_socket.close()
        
        # Wait for threads to complete
        for thread in self.threads:
            thread.join(timeout=2.0)
    def _print_views(self):
        """Print current server and client view"""
        server_list = [f"{s.uuid}@{s.ip}:{s.port}" for s in self.servers.values()]
        client_list = [f"{c.name}@{c.address[0]}:{c.address[1]}" for c in self.clients.values()]
        logger.info(f"\n[VIEW] 🔌 Servers ({len(server_list)}): {server_list}")
        logger.info(f"[VIEW] 👥 Clients ({len(client_list)}): {client_list}")
        logger.info(f"[VIEW] 👑 Current Leader: {self.leader_id}\n")

    def _setup_sockets(self):
        """Setup UDP and multicast sockets"""
        # Main UDP socket
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_socket.bind(('', self.port))
        self.udp_socket.settimeout(1.0)
        
        # Multicast socket for discovery
        self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.multicast_socket.bind(('', MULTICAST_PORT))
        
        # Join multicast group
        group = socket.inet_aton(MULTICAST_GROUP_IP)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        self.multicast_socket.setsockopt(
            socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq
        )
        self.multicast_socket.settimeout(1.0)
    
    def _initialize_election(self):
        """Initialize Bully election algorithm"""
        # Add ourselves to servers list
        local_ip = self._get_local_ip()
        self.servers[self.server_id] = ServerInfo(
            uuid=self.server_id,
            ip=local_ip,
            port=self.port,
            last_heartbeat=datetime.now()
        )
        
        self.bully_election = BullyElection(self.server_id, self.servers)
    
    def _get_local_ip(self) -> str:
        """Get local IP address"""
        try:
            # Connect to remote address to determine local IP
            temp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            temp_socket.connect(("8.8.8.8", 80))
            local_ip = temp_socket.getsockname()[0]
            temp_socket.close()
            return local_ip
        except Exception:
            return "127.0.0.1"
    
    def _start_threads(self):
        """Start all background threads"""
        threads_config = [
            (self._handle_udp_messages, "UDP Handler"),
            (self._handle_multicast_messages, "Multicast Handler"),
            (self._heartbeat_sender, "Heartbeat Sender"),
            (self._fault_monitor, "Fault Monitor")
        ]
        
        for target, name in threads_config:
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self.threads.append(thread)
    
    def _discover_servers(self):
        """Discover existing servers through multicast"""
        discovery_msg = Message(
            msg_type=MessageType.SERVER_DISCOVERY,
            sender_id=self.server_id,
            data={'port': self.port},
            timestamp=datetime.now()
        )
        
        try:
            self.multicast_socket.sendto(
                pickle.dumps(discovery_msg),
                (MULTICAST_GROUP_IP, MULTICAST_PORT)
            )
            logger.info("Sent server discovery message")
        except Exception as e:
            logger.error(f"Failed to send discovery message: {e}")
    
    def _handle_udp_messages(self):
        """Handle UDP messages from clients and servers"""
        while self.running:
            try:
                data, addr = self.udp_socket.recvfrom(1024)
                message = pickle.loads(data)
                
                if isinstance(message, Message):
                    self._process_message(message, addr)
                else:
                    logger.warning(f"Received invalid message format from {addr}")
                    
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    logger.error(f"UDP message handling error: {e}")
    
    def _handle_multicast_messages(self):
        """Handle multicast messages for server discovery"""
        while self.running:
            try:
                data, addr = self.multicast_socket.recvfrom(1024)
                message = pickle.loads(data)
                
                if isinstance(message, Message):
                    self._process_multicast_message(message, addr)
                    
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    logger.error(f"Multicast message handling error: {e}")
    
    def _process_message(self, message: Message, addr: Tuple[str, int]):
        """Process incoming messages"""
        msg_type = message.msg_type
        
        # Check for Byzantine faults
        if self.fault_detector.detect_byzantine_fault(message, {}):
            self._handle_byzantine_fault(message.sender_id)
            return
        
        # Route message based on type
        if msg_type == MessageType.CLIENT_JOIN:
            self._handle_client_join(message, addr)
        elif msg_type == MessageType.CLIENT_CHAT:
            self._handle_client_chat(message, addr)
        elif msg_type == MessageType.CLIENT_QUIT:
            self._handle_client_quit(message, addr)
        elif msg_type == MessageType.ELECTION:
            self.bully_election.handle_election_message(message, self.udp_socket, addr)
        elif msg_type == MessageType.OK:
            # Handle OK response in election
            pass
        elif msg_type == MessageType.COORDINATOR:
            self._handle_coordinator_message(message)
        elif msg_type == MessageType.HEARTBEAT:
            self._handle_heartbeat(message, addr)
        elif msg_type == MessageType.HEARTBEAT_ACK:
            self._handle_heartbeat_ack(message)
        else:
            logger.warning(f"Unknown message type: {msg_type}")
    
    def _process_multicast_message(self, message: Message, addr: Tuple[str, int]):
        """Process multicast discovery messages"""
        if message.msg_type == MessageType.SERVER_DISCOVERY:
            if message.sender_id != self.server_id:
                self._handle_server_discovery(message, addr)
        elif message.msg_type == MessageType.SERVER_ANNOUNCE:
            self._handle_server_announce(message, addr)
    
    def _handle_client_join(self, message: Message, addr: Tuple[str, int]):
        """Handle client join request"""
        if not self.is_leader:
            # Redirect to leader
            if self.leader_id and self.leader_id in self.servers:
                leader_info = self.servers[self.leader_id]
                redirect_msg = {
                    'redirect': True,
                    'leader_ip': leader_info.ip,
                    'leader_port': leader_info.port
                }
                self.udp_socket.sendto(pickle.dumps(redirect_msg), addr)
            return
        
        client_name = message.data.get('name', 'Unknown')
        client_id = f"{addr[0]}:{addr[1]}"
        
        self.clients[client_id] = ClientInfo(
            name=client_name,
            address=addr,
            join_time=datetime.now()
        )
        
        # Send confirmation
        response = {'status': 'joined', 'message': f'Welcome {client_name}!'}
        self.udp_socket.sendto(pickle.dumps(response), addr)
        
        # Broadcast join notification
        self._broadcast_to_clients(f"{client_name} joined the chat", exclude=addr)
        
        logger.info(f"Client {client_name} joined from {addr}")
    
    def _handle_client_chat(self, message: Message, addr: Tuple[str, int]):
        """Handle client chat message"""
        if not self.is_leader:
            return
        
        client_id = f"{addr[0]}:{addr[1]}"
        if client_id not in self.clients:
            return
        
        client_name = self.clients[client_id].name
        chat_message = message.data.get('message', '')
        
        # Broadcast message to all clients
        broadcast_msg = f"{client_name}: {chat_message}"
        self._broadcast_to_clients(broadcast_msg, exclude=addr)
        
        logger.info(f"Chat from {client_name}: {chat_message}")
    
    def _handle_client_quit(self, message: Message, addr: Tuple[str, int]):
        """Handle client quit"""
        client_id = f"{addr[0]}:{addr[1]}"
        if client_id in self.clients:
            client_name = self.clients[client_id].name
            del self.clients[client_id]
            
            # Broadcast quit notification
            self._broadcast_to_clients(f"{client_name} left the chat")
            
            logger.info(f"Client {client_name} quit")
    
    def _handle_server_discovery(self, message: Message, addr: Tuple[str, int]):
        """Handle server discovery message"""
        server_id = message.sender_id
        server_port = message.data.get('port', SERVER_PORT)
        
        # Add server to our list
        self.servers[server_id] = ServerInfo(
            uuid=server_id,
            ip=addr[0],
            port=server_port,
            last_heartbeat=datetime.now()

        )
        
        
        # Send our announcement
        announce_msg = Message(
            msg_type=MessageType.SERVER_ANNOUNCE,
            sender_id=self.server_id,
            data={'port': self.port},
            timestamp=datetime.now()
        )
        
        self.multicast_socket.sendto(
            pickle.dumps(announce_msg),
            (MULTICAST_GROUP_IP, MULTICAST_PORT)
        )
        
        logger.info(f"Discovered server {server_id} at {addr}")
        
        # Trigger election if we have a new server
        self.bully_election.start_election(self.udp_socket)
    
    def _handle_server_announce(self, message: Message, addr: Tuple[str, int]):
        """Handle server announcement"""
        server_id = message.sender_id
        server_port = message.data.get('port', SERVER_PORT)
        
        if server_id not in self.servers:
            self.servers[server_id] = ServerInfo(
                uuid=server_id,
                ip=addr[0],
                port=server_port,
                last_heartbeat=datetime.now()
            )
            
            logger.info(f"Added server {server_id} to server list")
    
    def _handle_coordinator_message(self, message: Message):
        """Handle coordinator announcement"""
        self.leader_id = message.data.get('coordinator')
        self.is_leader = (self.leader_id == self.server_id)
        
        if self.is_leader:
            logger.info(f"I am the leader: {self.server_id}")
        else:
            logger.info(f"New leader elected: {self.leader_id}")
    
    def _handle_heartbeat(self, message: Message, addr: Tuple[str, int]):
        """Handle heartbeat message"""
        server_id = message.sender_id
        
        if server_id in self.servers:
            self.servers[server_id].last_heartbeat = datetime.now()
        
        # Send heartbeat acknowledgment
        ack_msg = Message(
            msg_type=MessageType.HEARTBEAT_ACK,
            sender_id=self.server_id,
            data={},
            timestamp=datetime.now()
        )
        
        self.udp_socket.sendto(pickle.dumps(ack_msg), addr)
    
    def _handle_heartbeat_ack(self, message: Message):
        """Handle heartbeat acknowledgment"""
        server_id = message.sender_id
        if server_id in self.servers:
            self.servers[server_id].last_heartbeat = datetime.now()
    
    def _heartbeat_sender(self):
        """Send periodic heartbeats to other servers"""
        while self.running:
            try:
                heartbeat_msg = Message(
                    msg_type=MessageType.HEARTBEAT,
                    sender_id=self.server_id,
                    data={},
                    timestamp=datetime.now()
                )
                
                for server_id, server_info in self.servers.items():
                    if server_id != self.server_id:
                        try:
                            self.udp_socket.sendto(
                                pickle.dumps(heartbeat_msg),
                                (server_info.ip, server_info.port)
                            )
                        except Exception as e:
                            logger.error(f"Failed to send heartbeat to {server_id}: {e}")
                
                time.sleep(HEARTBEAT_INTERVAL)
                
            except Exception as e:
                logger.error(f"Heartbeat sender error: {e}")
    
    def _fault_monitor(self):
        """Monitor for server faults"""
        while self.running:
            try:
                # Check for crashed servers
                crashed_servers = self.fault_detector.detect_crash_fault(self.servers)
                
                for server_id in crashed_servers:
                    self._handle_server_crash(server_id)
                
                time.sleep(HEARTBEAT_INTERVAL)
                
            except Exception as e:
                logger.error(f"Fault monitor error: {e}")
    
    def _handle_server_crash(self, server_id: str):
        """Handle detected server crash"""
        if server_id in self.servers:
            logger.warning(f"Server {server_id} crashed")
            del self.servers[server_id]
            
            # If the crashed server was the leader, start election
            if server_id == self.leader_id:
                logger.info("Leader crashed, starting election")
                self.leader_id = None
                self.is_leader = False
                self.bully_election.start_election(self.udp_socket)
    
    def _handle_byzantine_fault(self, server_id: str):
        """Handle Byzantine fault detection"""
        logger.warning(f"Byzantine fault detected from {server_id}")
        # Implement Byzantine fault handling strategy
        # For now, just log and continue
    
    def _broadcast_to_clients(self, message: str, exclude: Tuple[str, int] = None):
        """Broadcast message to all connected clients"""
        for client_info in self.clients.values():
            if exclude and client_info.address == exclude:
                continue
            
            try:
                self.udp_socket.sendto(message.encode(UNICODE), client_info.address)
            except Exception as e:
                logger.error(f"Failed to send message to {client_info.name}: {e}")
    
    def _run_main_loop(self):
        """Main server loop"""
        try:
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.stop()

class ChatClient:
    """Simple chat client for testing"""
    
    def __init__(self, name: str, server_ip: str = '127.0.0.1', server_port: int = SERVER_PORT):
        self.name = name
        self.server_ip = server_ip
        self.server_port = server_port
        self.socket = None
        self.running = False
        
    def start(self):
        """Start the client"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.running = True
            
            # Join the chat
            join_msg = Message(
                msg_type=MessageType.CLIENT_JOIN,
                sender_id=self.name,
                data={'name': self.name},
                timestamp=datetime.now()
            )
            
            self.socket.sendto(
                pickle.dumps(join_msg),
                (self.server_ip, self.server_port)
            )
            
            # Start receiving messages
            receive_thread = threading.Thread(target=self._receive_messages, daemon=True)
            receive_thread.start()
            
            # Handle user input
            self._handle_input()
            
        except Exception as e:
            logger.error(f"Client error: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the client"""
        if self.running:
            self.running = False
            
            # Send quit message
            quit_msg = Message(
                msg_type=MessageType.CLIENT_QUIT,
                sender_id=self.name,
                data={},
                timestamp=datetime.now()
            )
            
            try:
                self.socket.sendto(
                    pickle.dumps(quit_msg),
                    (self.server_ip, self.server_port)
                )
            except:
                pass
            
            if self.socket:
                self.socket.close()
    
    def _receive_messages(self):
        """Receive messages from server"""
        while self.running:
            try:
                data, addr = self.socket.recvfrom(1024)
                
                try:
                    # Try to decode as Message object
                    message = pickle.loads(data)
                    if isinstance(message, dict):
                        if 'redirect' in message:
                            print(f"Redirected to leader: {message['leader_ip']}:{message['leader_port']}")
                            self.server_ip = message['leader_ip']
                            self.server_port = message['leader_port']
                        else:
                            print(f"Server: {message}")
                    else:
                        print(f"Received: {message}")
                except:
                    # Fall back to string decoding
                    message = data.decode(UNICODE)
                    print(message)
                    
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    logger.error(f"Client receive error: {e}")
    
    def _handle_input(self):
        """Handle user input"""
        print(f"Connected as {self.name}. Type messages or 'quit' to exit.")
        
        while self.running:
            try:
                user_input = input()
                
                if user_input.lower() == 'quit':
                    break
                
                # Send chat message
                chat_msg = Message(
                    msg_type=MessageType.CLIENT_CHAT,
                    sender_id=self.name,
                    data={'message': user_input},
                    timestamp=datetime.now()
                )
                
                self.socket.sendto(
                    pickle.dumps(chat_msg),
                    (self.server_ip, self.server_port)
                )
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Input handling error: {e}")

def main():
    """Main function to run server or client"""
    if len(sys.argv) < 2:
        print("Usage: python chat_system.py [server|client] [name]")
        sys.exit(1)
    
    mode = sys.argv[1].lower()
    
    if mode == 'server':
        port = int(sys.argv[2]) if len(sys.argv) > 2 else SERVER_PORT
        server = DistributedChatServer(port)
        server.start()
        server._print_views()
    elif mode == 'client':
        name = sys.argv[2] if len(sys.argv) > 2 else f"Client_{uuid.uuid4().hex[:8]}"
        client = ChatClient(name)
        client.start()
    else:
        print("Invalid mode. Use 'server' or 'client'")
        sys.exit(1)

if __name__ == "__main__":
    main()