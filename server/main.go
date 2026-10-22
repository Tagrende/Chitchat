package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"

	pb "chitchat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type chatServer struct {
	pb.UnimplementedChitChatServer
	logger *log.Logger
	nextID atomic.Uint64
}

func (s *chatServer) Connect(
	stream grpc.BidiStreamingServer[pb.ClientRequest, pb.ServerEvent],
) error {
	// The first request must be a join request.
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	join := req.GetJoin()
	if join == nil {
		return status.Error(codes.InvalidArgument, "first request must be a join request")
	}

	displayName := strings.TrimSpace(join.GetDisplayName())
	if displayName == "" {
		return status.Error(codes.InvalidArgument, "display name cannot be empty")
	}

	// Assign a unique ID to this participant.
	id := fmt.Sprintf("client-%d", s.nextID.Add(1))

	s.logger.Printf(
		"event=client_connected client_id=%s display_name=%q",
		id, displayName,
	)

	// Confirm the assigned ID to the joining client.
	err = stream.Send(&pb.ServerEvent{
		Event: &pb.ServerEvent_JoinConfirmation{
			JoinConfirmation: &pb.JoinConfirmation{
				ParticipantId: id,
				DisplayName:   displayName,
			},
		},
	})
	if err != nil {
		s.logger.Printf("event=join_confirmation_failed client_id=%s error=%v", id, err)
		return err
	}

	s.logger.Printf("event=join_confirmed client_id=%s", id)

	// Keep the connection open until the client disconnects or leaves.
	for {
		req, err := stream.Recv()

		if err == io.EOF {
			s.logger.Printf("event=client_disconnected client_id=%s", id)
			return nil
		}

		if err != nil {
			s.logger.Printf(
				"event=client_connection_error client_id=%s error=%v",
				id, err,
			)
			return err
		}

		switch {
		case req.GetLeave() != nil:
			s.logger.Printf("event=client_left client_id=%s", id)
			return nil

		case req.GetPublish() != nil:
			// Publishing will be implemented in a later step.
			err := stream.Send(&pb.ServerEvent{
				Event: &pb.ServerEvent_Error{
					Error: &pb.ErrorResponse{
						Message: "publishing is not implemented yet",
					},
				},
			})
			if err != nil {
				return err
			}

		case req.GetJoin() != nil:
			// A connection can only join once.
			err := stream.Send(&pb.ServerEvent{
				Event: &pb.ServerEvent_Error{
					Error: &pb.ErrorResponse{
						Message: "participant has already joined",
					},
				},
			})
			if err != nil {
				return err
			}

		default:
			return status.Error(codes.InvalidArgument, "request type is missing")
		}
	}
}

func main() {
	logger := log.New(os.Stdout, "component=Server ", log.LstdFlags)

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		logger.Fatalf("event=startup_failed error=%v", err)
	}

	server := grpc.NewServer()
	implementation := &chatServer{logger: logger}

	pb.RegisterChitChatServer(server, implementation)

	logger.Printf("event=server_started address=%s", listener.Addr())

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		logger.Println("event=server_shutting_down")
		server.GracefulStop()
	}()

	if err := server.Serve(listener); err != nil {
		logger.Fatalf("event=server_failed error=%v", err)
	}

	logger.Println("event=server_stopped")
}

// INFO:

// chatServer embeds pb.UnimplementedChitChatServer. This satisfies the generated interface's forward-compatibility requirement and lets us implement Connect next.

// net.Listen opens the TCP port for incoming connections.

// grpc.NewServer() creates the gRPC server.

// RegisterChitChatServer connects your implementation to the generated service.

// GracefulStop() allows active RPCs to finish before the server exits.
