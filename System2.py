# Enhanced Distributed Chat System with Improved Leader Recovery
# Features: Multi-client/multi-server, UDP, Bully algorithm, Enhanced Fault tolerance

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
CLIENT_RETRY_TIMEOUT = 2.0
MAX_CLIENT_RETRIES = 3

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
    
    # Enhanced leader management
    LEADER_CHANGE = 'LEADER_CHANGE'
    LEADER_HEARTBEAT = 'LEADER_HEARTBEAT'
    LEADER_DISCOVERY = 'LEADER_DISCOVERY'

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
        
        if sender not in self.byzantine_evidence:
            self.byzantine_evidence[sender] = []
        
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
        
        if len(evidence) > 10:
            evidence.pop(0)
        
        return False

class BullyElection:
    """Enhanced Bully algorithm with improved fault recovery"""
    
    def __init__(self, server_id: str, servers: Dict[str, ServerInfo]):
        self.server_id = server_id
        self.servers = servers
        self.election_in_progress = False
        self.coordinator = None
        self.election_timeout = ELECTION_TIMEOUT
        self.election_timer = None

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
        
        # Set election timeout
        if self.election_timer:
            self.election_timer.cancel()
        self.election_timer = threading.Timer(self.election_timeout, self.handle_election_timeout)
        self.election_timer.start()
        
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
        
        if self.election_timer:
            self.election_timer.cancel()
        
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
            logger.info("Election timeout - becoming coordinator")
            self.election_in_progress = False
            # This will be handled by the server's coordinator logic

class DistributedChatServer:
    """Enhanced server class with improved fault tolerance and leader recovery"""
    
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
        
        # Enhanced message handling
        self.pending_messages = []
        self.message_queue_lock = threading.Lock()
        
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
            
            # Wait a bit for discovery
            time.sleep(2)
            
            # Start election if we're the first server or no leader exists
            if len(self.servers) == 1 or not self.leader_id:
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
            (self._fault_monitor, "Fault Monitor"),
            (self._leader_heartbeat_sender, "Leader Heartbeat")
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
        """Enhanced message processing with election handling"""
        msg_type = message.msg_type
        
        # Check for Byzantine faults
        if self.fault_detector.detect_byzantine_fault(message, {}):
            self._handle_byzantine_fault(message.sender_id)
            return
        
        # Handle messages during election
        if self.bully_election.election_in_progress and msg_type in [
            MessageType.CLIENT_JOIN, MessageType.CLIENT_CHAT, MessageType.CLIENT_QUIT
        ]:
            self._queue_message_during_election(message, addr)
            return
        
        # Route message based on type
        if msg_type == MessageType.CLIENT_JOIN:
            self._handle_client_join(message, addr)
        elif msg_type == MessageType.CLIENT_CHAT:
            if sender_uuid not in client_view:
                client_view[sender_uuid] = ClientInfo(name='Unknown', address=addr, join_time=datetime.now())
                print(f"[Leader] 📥 Registered rejoining client: {sender_uuid}")
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
        elif msg_type == MessageType.LEADER_DISCOVERY:
            self._handle_leader_discovery(message, addr)
        else:
            logger.warning(f"Unknown message type: {msg_type}")

    def _queue_message_during_election(self, message: Message, addr: Tuple[str, int]):
        """Queue client messages during election"""
        with self.message_queue_lock:
            self.pending_messages.append((message, addr))
            logger.info(f"Queued message during election: {message.msg_type}")

    def _process_pending_messages(self):
        """Process queued messages after election completes"""
        with self.message_queue_lock:
            if self.is_leader and self.pending_messages:
                logger.info(f"Processing {len(self.pending_messages)} pending messages")
                for message, addr in self.pending_messages:
                    self._process_message(message, addr)
                self.pending_messages.clear()

    def _handle_client_join(self, message: Message, addr: Tuple[str, int]):
        """Enhanced client join handling with leader redirection"""
        if not self.is_leader:
            self._redirect_to_leader(addr)
            return
        
        client_name = message.data.get('name', 'Unknown')
        client_id = f"{addr[0]}:{addr[1]}"
        
        self.clients[client_id] = ClientInfo(
            name=client_name,
            address=addr,
            join_time=datetime.now()
        )
        
        # Send confirmation with leader info
        response = {
            'status': 'joined',
            'message': f'Welcome {client_name}!',
            'leader_id': self.server_id,
            'leader_ip': self._get_local_ip(),
            'leader_port': self.port
        }
        self.udp_socket.sendto(pickle.dumps(response), addr)
        
        # Broadcast join notification
        self._broadcast_to_clients(f"{client_name} joined the chat", exclude=addr)
        logger.info(f"Client {client_name} joined from {addr}")

    def _handle_client_chat(self, message: Message, addr: Tuple[str, int]):
        """Enhanced chat handling with leader check"""
        if not self.is_leader:
            self._redirect_to_leader(addr)
            return
        
        client_id = f"{addr[0]}:{addr[1]}"
        if client_id not in self.clients:
            # Client not registered, redirect to join first
            response = {'error': 'Please join the chat first'}
            self.udp_socket.sendto(pickle.dumps(response), addr)
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

    def _redirect_to_leader(self, client_addr: Tuple[str, int]):
        """Redirect client to current leader"""
        if self.leader_id and self.leader_id in self.servers:
            leader_info = self.servers[self.leader_id]
            redirect_msg = {
                'redirect': True,
                'leader_ip': leader_info.ip,
                'leader_port': leader_info.port,
                'leader_id': self.leader_id
            }
        else:
            # No leader available, trigger election
            redirect_msg = {
                'error': 'No leader available, please retry',
                'retry_after': 3
            }
            if not self.bully_election.election_in_progress:
                self.bully_election.start_election(self.udp_socket)
        
        try:
            self.udp_socket.sendto(pickle.dumps(redirect_msg), client_addr)
        except Exception as e:
            logger.error(f"Failed to send redirect message: {e}")

    def _handle_coordinator_message(self, message: Message):
        """Enhanced coordinator message handling"""
        old_leader = self.leader_id
        self.leader_id = message.data.get('coordinator')
        self.is_leader = (self.leader_id == self.server_id)
        
        if self.is_leader:
            logger.info(f"I am the new leader: {self.server_id}")
            self._notify_clients_of_new_leader()
            self._process_pending_messages()
        else:
            logger.info(f"New leader elected: {self.leader_id}")
        
        # Update election state
        self.bully_election.election_in_progress = False
        self.bully_election.coordinator = self.leader_id

    def _notify_clients_of_new_leader(self):
        """Notify all clients about new leader"""
        if self.is_leader:
            leader_announcement = {
                'type': 'LEADER_CHANGE',
                'new_leader_id': self.server_id,
                'new_leader_ip': self._get_local_ip(),
                'new_leader_port': self.port,
                'message': 'New leader elected - chat service restored'
            }
            
            self._broadcast_to_clients_raw(pickle.dumps(leader_announcement))
            logger.info("Notified all clients of new leader")

    def _leader_heartbeat_sender(self):
        """Send periodic leader heartbeat to clients"""
        while self.running:
            try:
                if self.is_leader and self.clients:
                    heartbeat_msg = {
                        'type': 'LEADER_HEARTBEAT',
                        'leader_id': self.server_id,
                        'timestamp': datetime.now().isoformat(),
                        'client_count': len(self.clients)
                    }
                    
                    self._broadcast_to_clients_raw(pickle.dumps(heartbeat_msg))
                
                time.sleep(HEARTBEAT_INTERVAL * 2)  # Less frequent than server heartbeats
                
            except Exception as e:
                logger.error(f"Leader heartbeat error: {e}")

    def _handle_leader_discovery(self, message: Message, addr: Tuple[str, int]):
        """Handle leader discovery requests from clients"""
        if self.is_leader:
            response = {
                'leader_found': True,
                'leader_id': self.server_id,
                'leader_ip': self._get_local_ip(),
                'leader_port': self.port
            }
        elif self.leader_id and self.leader_id in self.servers:
            leader_info = self.servers[self.leader_id]
            response = {
                'leader_found': True,
                'leader_id': self.leader_id,
                'leader_ip': leader_info.ip,
                'leader_port': leader_info.port
            }
        else:
            response = {
                'leader_found': False,
                'message': 'No leader available, election in progress'
            }
        
        try:
            self.udp_socket.sendto(pickle.dumps(response), addr)
        except Exception as e:
            logger.error(f"Failed to send leader discovery response: {e}")

    def _handle_server_crash(self, server_id: str):
        """Enhanced server crash handling"""
        if server_id in self.servers:
            logger.warning(f"Server {server_id} crashed")
            del self.servers[server_id]
            
            # If the crashed server was the leader
            if server_id == self.leader_id:
                logger.info("Leader crashed, initiating recovery")
                self.leader_id = None
                self.is_leader = False
                
                # Notify clients about leader loss
                self._notify_clients_leader_lost()
                
                # Start immediate election
                self.bully_election.start_election(self.udp_socket)
                
                # Set recovery timer
                threading.Timer(3.0, self._check_election_completion).start()

    def _notify_clients_leader_lost(self):
        """Notify clients that leader is lost"""
        leader_lost_msg = {
            'type': 'LEADER_LOST',
            'message': 'Leader crashed - new leader election in progress',
            'retry_after': 5
        }
        
        self._broadcast_to_clients_raw(pickle.dumps(leader_lost_msg))
        logger.info("Notified clients of leader loss")

    def _check_election_completion(self):
        """Check if election completed and handle timeout"""
        if not self.leader_id and not self.bully_election.election_in_progress:
            logger.warning("Election timeout - becoming coordinator")
            self.bully_election.become_coordinator(self.udp_socket)
            self.is_leader = True
            self.leader_id = self.server_id

    def _broadcast_to_clients_raw(self, data: bytes):
        """Broadcast raw data to all connected clients"""
        for client_info in self.clients.values():
            try:
                self.udp_socket.sendto(data, client_info.address)
            except Exception as e:
                logger.error(f"Failed to send raw message to {client_info.name}: {e}")

    def _broadcast_to_clients(self, message: str, exclude: Tuple[str, int] = None):
        """Broadcast message to all connected clients"""
        for client_info in self.clients.values():
            if exclude and client_info.address == exclude:
                continue
            try:
                self.udp_socket.sendto(message.encode(UNICODE), client_info.address)
            except Exception as e:
                logger.error(f"Failed to send message to {client_info.name}: {e}")

    # ... (rest of the methods remain the same as in original code)
    def _process_multicast_message(self, message: Message, addr: Tuple[str, int]):
        """Process multicast discovery messages"""
        if message.msg_type == MessageType.SERVER_DISCOVERY:
            if message.sender_id != self.server_id:
                self._handle_server_discovery(message, addr)
        elif message.msg_type == MessageType.SERVER_ANNOUNCE:
            self._handle_server_announce(message, addr)

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
        if not self.leader_id:
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

    def _handle_byzantine_fault(self, server_id: str):
        """Handle Byzantine fault detection"""
        logger.warning(f"Byzantine fault detected from {server_id}")
        # Implement Byzantine fault handling strategy

    def _print_views(self):
        """Print current server and client view"""
        server_list = [f"{s.uuid}@{s.ip}:{s.port}" for s in self.servers.values()]
        client_list = [f"{c.name}@{c.address[0]}:{c.address[1]}" for c in self.clients.values()]
        
        logger.info(f"\n[VIEW] 🔌 Servers ({len(server_list)}): {server_list}")
        logger.info(f"[VIEW] 👥 Clients ({len(client_list)}): {client_list}")
        logger.info(f"[VIEW] 👑 Current Leader: {self.leader_id}\n")

    def _run_main_loop(self):
        """Main server loop"""
        try:
            while self.running:
                time.sleep(1)
                # Periodic view printing
                if time.time() % 10 < 1:  # Every 10 seconds
                    self._print_views()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.stop()

class ChatClient:
    """Enhanced chat client with automatic leader discovery and retry"""
    
    def __init__(self, name: str, server_ip: str = '127.0.0.1', server_port: int = SERVER_PORT):
        self.name = name
        self.server_ip = server_ip
        self.server_port = server_port
        self.socket = None
        self.running = False
        self.retry_count = 0
        self.max_retries = MAX_CLIENT_RETRIES
        self.backup_servers = []

    def start(self):
        """Start the client with enhanced error handling"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.settimeout(CLIENT_RETRY_TIMEOUT)
            self.running = True
            
            # Try to join the chat with retry
            if self._join_chat_with_retry():
                # Start receiving messages
                receive_thread = threading.Thread(target=self._receive_messages, daemon=True)
                receive_thread.start()
                
                # Handle user input
                self._handle_input()
            else:
                print("Failed to join chat after multiple attempts")
                
        except Exception as e:
            logger.error(f"Client error: {e}")
        finally:
            self.stop()

    def _join_chat_with_retry(self) -> bool:
        """Join chat with automatic retry and leader discovery"""
        for attempt in range(self.max_retries):
            try:
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
                
                # Wait for response
                data, addr = self.socket.recvfrom(1024)
                response = pickle.loads(data)
                
                if isinstance(response, dict):
                    if response.get('redirect'):
                        # Redirected to leader
                        self.server_ip = response['leader_ip']
                        self.server_port = response['leader_port']
                        print(f"Redirected to leader: {self.server_ip}:{self.server_port}")
                        continue  # Retry with new leader
                    elif response.get('status') == 'joined':
                        print(f"Successfully joined chat: {response.get('message')}")
                        return True
                    elif response.get('error'):
                        print(f"Join error: {response['error']}")
                        if 'retry_after' in response:
                            time.sleep(response['retry_after'])
                        continue
                
            except socket.timeout:
                print(f"Join attempt {attempt + 1} timed out")
                if attempt < self.max_retries - 1:
                    self._discover_leader()
            except Exception as e:
                print(f"Join attempt {attempt + 1} failed: {e}")
        
        return False

    def _discover_leader(self):
        """Discover current leader"""
        try:
            discovery_msg = Message(
                msg_type=MessageType.LEADER_DISCOVERY,
                sender_id=self.name,
                data={},
                timestamp=datetime.now()
            )
            
            self.socket.sendto(
                pickle.dumps(discovery_msg),
                (self.server_ip, self.server_port)
            )
            
            data, addr = self.socket.recvfrom(1024)
            response = pickle.loads(data)
            
            if response.get('leader_found'):
                self.server_ip = response['leader_ip']
                self.server_port = response['leader_port']
                print(f"Discovered leader: {self.server_ip}:{self.server_port}")
            else:
                print("No leader available, retrying...")
                
        except Exception as e:
            print(f"Leader discovery failed: {e}")

    def _send_message_with_retry(self, message: Message) -> bool:
        """Send message with automatic retry"""
        for attempt in range(self.max_retries):
            try:
                self.socket.sendto(
                    pickle.dumps(message),
                    (self.server_ip, self.server_port)
                )
                return True
                
            except Exception as e:
                print(f"Send attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    self._discover_leader()
                    time.sleep(1)
        
        return False

    def _receive_messages(self):
        """Enhanced message receiving with leader change handling"""
        while self.running:
            try:
                data, addr = self.socket.recvfrom(1024)
                
                try:
                    message = pickle.loads(data)
                    if isinstance(message, dict):
                        msg_type = message.get('type')
                        
                        if msg_type == 'LEADER_CHANGE':
                            self.server_ip = message['new_leader_ip']
                            self.server_port = message['new_leader_port']
                            print(f"\n🔄 {message.get('message', 'Leader changed')}")
                            print(f"New leader: {self.server_ip}:{self.server_port}")
                            
                        elif msg_type == 'LEADER_LOST':
                            print(f"\n⚠️  {message.get('message', 'Leader lost')}")
                            
                        elif msg_type == 'LEADER_HEARTBEAT':
                            # Silent heartbeat processing
                            pass
                            
                        elif 'redirect' in message:
                            self.server_ip = message['leader_ip']
                            self.server_port = message['leader_port']
                            print(f"Redirected to: {self.server_ip}:{self.server_port}")
                            
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
        """Enhanced input handling with retry"""
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
                
                if not self._send_message_with_retry(chat_msg):
                    print("Failed to send message after retries")
                    
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Input handling error: {e}")

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

def main():
    """Main function to run server or client"""
    if len(sys.argv) < 2:
        print("Usage: python chat_system.py [server|client] [name/port]")
        sys.exit(1)
    
    mode = sys.argv[1].lower()
    
    if mode == 'server':
        port = int(sys.argv[2]) if len(sys.argv) > 2 else SERVER_PORT
        server = DistributedChatServer(port)
        server.start()
        
    elif mode == 'client':
        name = sys.argv[2] if len(sys.argv) > 2 else f"Client_{uuid.uuid4().hex[:8]}"
        client = ChatClient(name)
        client.start()
        
    else:
        print("Invalid mode. Use 'server' or 'client'")
        sys.exit(1)

if __name__ == "__main__":
    main()
 try:
     def retry_discovery_and_connect(client_uuid, name, client_socket):
    for _ in range(MAX_CLIENT_RETRIES):
        print("[Client] 🔄 Attempting to rediscover leader...")
        send_discovery_message(client_uuid, name, client_socket)
        time.sleep(CLIENT_RETRY_TIMEOUT)
    print("[Client] ❌ Could not reconnect after leader failure.")

def send_discovery_message(client_uuid, name, client_socket):
    join_message = {
        'type': MessageType.CLIENT_JOIN.value,
        'uuid': client_uuid,
        'name': name
    }
    data = pickle.dumps(join_message)
    client_socket.sendto(data, (MULTICAST_GROUP_IP, MULTICAST_PORT))
 except Exception as e:
     print('[Client] ❌ Failed to send. Retrying...')
     retry_discovery_and_connect(client_uuid, name, client_socket)
