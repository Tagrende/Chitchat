# Chit chat
Chit Chat is a distributed chat system where participants can join, exchange messages, leave the conversation at any time, and receive broadcasts from other participants. The system assigns logical timestamps to all events using a Lamport-style logical clock.

The application is implemented in Go using gRPC and Protocol Buffers. The system consists of a single server and muliple client processes communicating through a bidirectional gRPC stream

## Running the project
Clone or download the repository and open a terminal in the project root directory.
### Running the server
Start the server by running:
go run ./server<br/>
The server will start listening for incoming client connections on port 50051.

### Running the clients
Open a new terminal and run:
go run ./client<br/>
When prompted, enter a display name:
Enter your display name:<br/>
The client will connect to the server and receive an assigned participant ID.

To connect additional participants, open more terminal windows and run:
go run ./client<br/>
For each client. All connected clients will receive join notifications, chat messages, and leave notifications.

After joining the chat, type a message and press Enter. The message will be broadcast to all connected participants together with its logical timestamp.

To leave the chat, type:
/leave<br/>
The server will broadcast a leave notification to all remaining participants.
