package consumer

import (
	"github.com/shubhang93/relcon/internal/offman"
	"github.com/shubhang93/relcon/internal/toppar"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func makeRebalanceCB(man *offman.Manager) func(consumer *kafka.Consumer, event kafka.Event) error {
	return func(consumer *kafka.Consumer, event kafka.Event) error {
		switch rbcEvent := event.(type) {
		case kafka.AssignedPartitions:
			log.Println("[re-balance CB]:", rbcEvent)
			err := consumer.Resume(rbcEvent.Partitions)
			if err != nil {
				return err
			}

		case kafka.RevokedPartitions:
			partTracks := man.CommittableMessages()
			log.Printf("[revoked parts]:%v\n", rbcEvent.Partitions)
			for _, ktp := range rbcEvent.Partitions {
				tp := toppar.KafkaTopicPartToTopicPart(ktp)
				partTrack, ok := partTracks[tp]
				if ok {
					storedOff, err := consumer.StoreMessage(partTrack.Message)
					if err != nil {
						log.Println("[re-balance CB]:error storing offset", err)
						continue

					}
					log.Println("[revoked parts]: storing offsets:", storedOff)
				}
			}

			_, err := consumer.Commit()
			if err != nil && err.(kafka.Error).Code() != kafka.ErrNoOffset {
				return err
			}

			man.ClearPartitions(toppar.KafkaTopPartsToTopParts(rbcEvent.Partitions))
		default:
			log.Printf("[%s]:unknown rebalance event\n", consumer.String())
		}
		return nil
	}
}
