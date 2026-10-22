package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	pb "chitchat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger := log.New(os.Stdout, "component=Client ", log.LstdFlags)

	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Enter your display name: ")
	name, err := reader.ReadString('\n')
	if err != nil {
		logger.Fatalf("event=input_failed error=%v", err)
	}

	name = strings.TrimSpace(name)
	if name == "" {
		logger.Fatal("event=join_failed reason=empty_display_name")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		logger.Fatalf("event=connection_failed error=%v", err)
	}
	defer conn.Close()

	client := pb.NewChitChatClient(conn)

	stream, err := client.Connect(context.Background())
	if err != nil {
		logger.Fatalf("event=stream_failed error=%v", err)
	}

	err = stream.Send(&pb.ClientRequest{
		Request: &pb.ClientRequest_Join{
			Join: &pb.JoinRequest{
				DisplayName: name,
			},
		},
	})
	if err != nil {
		logger.Fatalf("event=join_failed error=%v", err)
	}

	event, err := stream.Recv()
	if err != nil {
		logger.Fatalf("event=join_confirmation_failed error=%v", err)
	}

	confirmation := event.GetJoinConfirmation()
	if confirmation == nil {
		logger.Fatalf("event=join_confirmation_invalid")
	}

	clientID := confirmation.GetParticipantId()

	logger.Printf(
		"event=joined client_id=%s display_name=%q",
		clientID,
		confirmation.GetDisplayName(),
	)

	fmt.Printf(
		"\nWelcome, %s! Your assigned ID is %s.\n",
		confirmation.GetDisplayName(),
		clientID,
	)

	fmt.Println("You are connected. Press Ctrl+C to exit.")

	fmt.Println("Type /leave to leave Chit Chat.")

	go func() {
		for {
			input, err := reader.ReadString('\n')
			if err != nil {
				return
			}

			command := strings.TrimSpace(input)

			if command == "/leave" {
				err := stream.Send(&pb.ClientRequest{
					Request: &pb.ClientRequest_Leave{
						Leave: &pb.LeaveRequest{},
					},
				})
				if err != nil {
					logger.Printf(
						"event=leave_request_failed client_id=%s error=%v",
						clientID, err,
					)
				}
				return
			}

			fmt.Println("Chat publishing isn't implemented yet. Type /leave to exit.")
		}
	}()

	// Keep receiving server events.
	for {
		event, err := stream.Recv()
		if err != nil {
			logger.Printf("event=disconnected client_id=%s error=%v", clientID, err)
			return
		}

		if broadcast := event.GetBroadcast(); broadcast != nil {
			fmt.Printf(
				"[%d] %s: %s\n",
				broadcast.GetLogicalTimestamp(),
				broadcast.GetDisplayName(),
				broadcast.GetContent(),
			)

			logger.Printf(
				"event=message_received client_id=%s logical_timestamp=%d content=%q",
				clientID,
				broadcast.GetLogicalTimestamp(),
				broadcast.GetContent(),
			)
		}

		if serverError := event.GetError(); serverError != nil {
			fmt.Printf("Server error: %s\n", serverError.GetMessage())
		}
	}
}
