import argparse
import threading
import logging
import json
import socket
from http.server import HTTPServer, BaseHTTPRequestHandler
import time

class Node:
    def __init__(self, http_port, p2p_port, address=None, handicap=0):
        self.http_port = http_port
        self.p2p_port = p2p_port
        self.address = address
        self.my_id = f'localhost:{self.p2p_port}'
        self.handicap = handicap
        self.doneFlag = False
        self.peers = []
        self.tasks = {}
        self.results = {}
        self.stats = {'tasks_completed': 0, 'validations_done': 0, 'puzzles_solved': 0}
        self.node_stats = {}
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('0.0.0.0', self.p2p_port))
        self.peers_lock = threading.Lock()
        self.expected_results = 0
        self.sudoku = None

    def get_network_info(self):
        network_info = {}
        with self.peers_lock:
            for addr in self.peers:
                network_info[addr] = self.peers
        return network_info

    def divide_and_assign(self, sudoku):
        if not self.peers:
            logging.warning("No peers available to assign tasks. Attempting to solve locally...")
            solution = self.solve_sudoku(sudoku)
            if solution:
                logging.info("Sudoku solved locally.")
                print(solution)
                return
            else:
                logging.error("Failed to solve Sudoku locally.")
                return

        self.sudoku = sudoku
        empty_cells = [(i, j) for i in range(9) for j in range(9) if sudoku[i][j] == 0]
        self.expected_results = len(empty_cells)

        for idx, cell in enumerate(empty_cells):
            peer_index = idx % len(self.peers)
            peer = self.peers[peer_index]
            self.send_message(peer, {'type': 'TASK', 'task_id': idx, 'cell': cell, 'sudoku': sudoku})

    def solve_sudoku(self, sudoku):
        def solve(board):
            find = self.find_empty(board)
            if not find:
                return True
            else:
                row, col = find

            for i in range(1, 10):
                if self.is_valid(board, i, (row, col)):
                    board[row][col] = i

                    if solve(board):
                        return True

                    board[row][col] = 0

            return False

        board = [row[:] for row in sudoku]
        if solve(board):
            return board
        else:
            return None

    def solve_sudoku_cell(self, sudoku, cell):
        row, col = cell
        possible_numbers = []
        for num in range(1, 10):
            if self.is_valid(sudoku, num, (row, col)):
                possible_numbers.append(num)
        return possible_numbers

    def is_valid(self, board, num, pos):
        for i in range(len(board[0])):
            if board[pos[0]][i] == num and pos[1] != i:
                return False
        for i in range(len(board)):
            if board[i][pos[1]] == num and pos[0] != i:
                return False
        box_x = pos[1] // 3
        box_y = pos[0] // 3
        for i in range(box_y * 3, box_y * 3 + 3):
            for j in range(box_x * 3, box_x * 3 + 3):
                if board[i][j] == num and (i, j) != pos:
                    return False
        return True

    def find_empty(self, board):
        for i in range(len(board)):
            for j in range(len(board[0])):
                if board[i][j] == 0:
                    return (i, j)
        return None

    def fill_single_possibilities(self):
        updated = False
        for cell, possible_numbers in list(self.results.items()):
            if len(possible_numbers) == 1:
                row, col = cell
                self.sudoku[row][col] = possible_numbers[0]
                del self.results[cell]
                updated = True
        return updated

    def combine_results_and_resolve(self):
        while self.fill_single_possibilities():
            self.results = {}
            self.divide_and_assign(self.sudoku)

        empty_cells = [(i, j) for i in range(9) for j in range(9) if self.sudoku[i][j] == 0]
        if not empty_cells:
            return self.sudoku

        return None

    def track_solved_puzzle(self):
        self.stats['puzzles_solved'] += 1

    def track_validations(self):
        self.stats['validations_done'] += 1

    def track_node_validations(self, address):
        if address in self.peers or address == self.my_id:
            if address in self.node_stats:
                self.node_stats[address] += 1
            else:
                self.node_stats[address] = 1


    def get_overall_stats(self):
        return self.stats

    def get_node_stats(self, address):
        return self.node_stats.get(address, 0)

    def run_p2p_server(self):
        try:
            logging.info(f'Starting P2P server on port {self.p2p_port}')
            while True:
                data, addr = self.sock.recvfrom(1024)
                message = json.loads(data.decode('utf-8'))
                threading.Thread(target=self.handle_message, args=(message, addr)).start()
        except KeyboardInterrupt:
            logging.info("P2P server shutting down...")
        finally:
            self.sock.close()

    def join_network(self, address):
        if isinstance(address, tuple):
            address = f"{address[0]}:{address[1]}"
        self.send_message(address, {'type': 'JOIN', 'address': self.my_id})
        logging.info(f"Sent JOIN message to {address}")

    def handle_message(self, message, addr):
        logging.info(f"Received message from {addr}: {message}")

        if message['type'] == 'JOIN':
            with self.peers_lock:
                if message['address'] not in self.peers:
                    self.peers.append(message['address'])
            self.node_stats[message['address']] = 0  # Inicializa contador de validações para o nó que acabou de se juntar
            logging.info(f"Node {message['address']} joined the network")
            self.notify_peers(message['address'])
        elif message['type'] == 'LEAVE':
            with self.peers_lock:
                if message['address'] in self.peers:
                    self.peers.remove(message['address'])
                    self.node_stats.pop(message['address'], None)
                    for peer in self.peers:
                        self.send_message(peer, {'type': 'LEAVE', 'address': message['address']})
        elif message['type'] == 'TASK':
            if self.handicap > 0:
                time.sleep(self.handicap / 1000)
            cell = tuple(message['cell'])  # Ensure cell is a tuple
            sudoku = message['sudoku']
            possible_numbers = self.solve_sudoku_cell(sudoku, cell)
            self.send_message(addr, {'type': 'RESULT', 'task_id': message['task_id'], 'cell': cell, 'possible_numbers': possible_numbers})
            self.track_node_validations(self.my_id)  # Track node validation done for the local node
        elif message['type'] == 'RESULT':
            if 'task_id' in message:
                self.results[tuple(message['cell'])] = message['possible_numbers']
                self.track_validations()  # Track validation done
                self.track_node_validations(message['task_id'])  # Track node validation done for the task ID
                if len(self.results) == self.expected_results:
                    combined_sudoku = self.combine_results_and_resolve()
                    if combined_sudoku:
                        logging.info("Sudoku solved and validated successfully.")
                        self.track_solved_puzzle()  # Track puzzle solved
                        self.solved_sudoku = combined_sudoku
                    else:
                        logging.info("Reassigning tasks after filling single possibilities...")
                        self.results = {}
                        self.divide_and_assign(self.sudoku)
        elif message['type'] == 'STATS':
            self.send_message(addr[0], {'type': 'STATS', 'stats': self.stats})
        elif message['type'] == 'NETWORK':
            self.send_message(addr[0], {'type': 'NETWORK', 'peers': self.peers})

        elif message['type'] == 'STATS':
            self.send_message(addr[0], {'type': 'STATS', 'stats': self.stats})
        elif message['type'] == 'NETWORK':
            self.send_message(addr[0], {'type': 'NETWORK', 'peers': self.peers})


    def notify_peers(self, new_peer_address):
        with self.peers_lock:
            for peer in self.peers:
                if peer != new_peer_address:
                    self.send_message(peer, {'type': 'JOIN', 'address': new_peer_address})

    def send_message(self, peer, message):
        if isinstance(peer, tuple):
            peer_address, peer_port = peer
        else:
            peer_address, peer_port = peer.split(':')
        self.sock.sendto(json.dumps(message).encode('utf-8'), (peer_address, int(peer_port)))
        logging.info(f"Sent message to {peer_address}:{peer_port}: {message}")

    def run(self):
        logging.info(f"Node {self.my_id} started")
        if self.address:
            self.join_network(self.address)
        while not self.doneFlag:
            try:
                payload, address = self.recv()
                if payload:
                    message = json.loads(payload)
                    self.handle_message(message, address)
            except KeyboardInterrupt:
                self.done()
            except Exception as e:
                logging.error("Error in run: " + str(e))

    def done(self):
        self.doneFlag = True
        with self.peers_lock:
            for peer in self.peers:
                self.send_message(peer, {'type': 'LEAVE', 'address': self.my_id})
        if self.address:
            self.send_message(self.address, {'type': 'LEAVE', 'address': self.my_id})
        logging.info("Node shutting down...")

    def recv(self):
        try:
            payload, address = self.sock.recvfrom(1024)
            return payload, address
        except Exception as e:
            logging.error("Error in recv: " + str(e))
            return None, None

class SudokuHandler(BaseHTTPRequestHandler):
    def __init__(self, node, *args, **kwargs):
        self.node = node
        super().__init__(*args, **kwargs)

    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()

    def do_POST(self):
        if self.path == "/solve":
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            sudoku_data = json.loads(post_data.decode('utf-8'))
            self.node.divide_and_assign(sudoku_data["sudoku"])

            # Aguarda até que o Sudoku seja resolvido localmente no nó principal
            while not hasattr(self.node, 'solved_sudoku'):
                time.sleep(0.1)  # Adiciona uma pequena espera para evitar espera ativa
            self._set_response()
            response_message = "\nSudoku solved successfully! Solution:\n"
            response_message += json.dumps(self.node.solved_sudoku) + '\n'
            self.wfile.write(response_message.encode('utf-8'))
            del self.node.solved_sudoku  # Limpa o atributo solved_sudoku para a próxima requisição

    def do_GET(self):
        if self.path == "/stats":
            all_solved = self.node.get_overall_stats()["puzzles_solved"]
            all_validations = self.node.get_overall_stats()["validations_done"]
            nodes_stats = [{"address": addr, "validations": self.node.get_node_stats(addr)} for addr in self.node.peers] 
            stats_data = {
                "all": {
                    "solved": all_solved,
                    "validations": all_validations
                },
                "nodes": nodes_stats
            }
            self._set_response()
            self.wfile.write(json.dumps(stats_data).encode('utf-8'))
        elif self.path == "/network":
            network_data = self.node.get_network_info()
            self._set_response()
            self.wfile.write(json.dumps(network_data).encode('utf-8'))

    def log_message(self, format, *args):
        return  # Disable default HTTP logging

def run_http_server(node, http_port):
    server_address = ("localhost", http_port)
    httpd = HTTPServer(server_address, lambda *args, **kwargs: SudokuHandler(node, *args, **kwargs))
    logging.info(f"Starting HTTP server on {server_address}")
    httpd.serve_forever()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Sudoku Solver Node', conflict_handler='resolve')
    parser.add_argument('-p', '--http_port', type=int, required=True, help='HTTP port')
    parser.add_argument('-s', '--p2p_port', type=int, required=True, help='P2P port')
    parser.add_argument('-a', '--address', type=str, required=False, help='Anchor node address (host:port)')
    parser.add_argument('-h', '--handicap', type=float, required=False, default=0, help='Handicap (delay in ms) for validation')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    node = Node(args.http_port, args.p2p_port, args.address, args.handicap)
    http_thread = threading.Thread(target=run_http_server, args=(node, args.http_port))
    http_thread.daemon = True
    http_thread.start()
    node.run()