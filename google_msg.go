package SQLConn

import (
	"context"
	"fmt"
	"log"

	firebase "firebase.google.com/go"
	"firebase.google.com/go/messaging"

	"google.golang.org/api/option"
)

type SendGoogleMessage struct {
	cli *messaging.Client
}

func (sgm *SendGoogleMessage) Init(filename string) error {

	opt := option.WithCredentialsFile(filename)
	config := &firebase.Config{ProjectID: "easypapertracker"}
	app, err := firebase.NewApp(context.Background(), config, opt)
	if err != nil {
		return err
	}
	log.Print("google cloud message is ok ")
	sgm.cli, err = app.Messaging(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func (sgm *SendGoogleMessage) sendMessageToTopic(topicID string, topicStr string) (string, error) {
	ret, err := sgm.cli.Send(context.Background(), &messaging.Message{
		Topic: topicID,
		Data: map[string]string{
			"type": "update",
		},
		Notification: &messaging.Notification{
			Title: fmt.Sprintf("%s has new content", topicStr),
			Body:  fmt.Sprintf("The topic you subscribe to - %s has new content and will be automatically updated with one click.", topicStr),
		},
	})
	if err != nil {
		return "", err
	}
	return ret, nil
}
