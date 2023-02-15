package consumer

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/shubhang93/relcon/internal/debug"
	"github.com/shubhang93/relcon/internal/toppar"
	"github.com/shubhang93/relcon/offman"
)

const pollTimeoutMS = 100

type OffManConsumer struct {
	kafCon           *kafka.Consumer
	wg               sync.WaitGroup
	offMan           *offman.Manager
	lastCommit       time.Time
	sendChan         chan []*kafka.Message
	batchSize        int
	commitIntervalMS time.Duration
}

func New(conf Config, messageIn chan []*kafka.Message) *OffManConsumer {
	config := conf.toConfigMap()
	c, err := kafka.NewConsumer(&config)
	if err != nil {
		panic(err)
	}

	batchSize := 500
	if conf.MessageBatchSize > 0 {
		batchSize = conf.MessageBatchSize
	}

	commitIntervalMS := 5000
	if conf.CommitIntervalMS > 0 {
		commitIntervalMS = conf.CommitIntervalMS
	}

	return &OffManConsumer{
		kafCon:           c,
		offMan:           offman.New(),
		lastCommit:       time.Now(),
		sendChan:         messageIn,
		batchSize:        batchSize,
		commitIntervalMS: time.Duration(commitIntervalMS),
	}
}

func (omc *OffManConsumer) pollBatch(ctx context.Context, timeoutInMs int) ([]*kafka.Message, error) {
	messages := make([]*kafka.Message, 0, omc.batchSize)
	remainingTime := time.Duration(timeoutInMs) * time.Millisecond
	endTime := time.Now().Add(time.Duration(timeoutInMs) * time.Millisecond)

	done := ctx.Done()
	for len(messages) < omc.batchSize {
		select {
		case <-done:
			log.Printf("[consumer shutdown]: starting shutdown, received context done")
			return messages, ctx.Err()
		default:
			e := omc.kafCon.Poll(timeoutInMs)
			switch event := e.(type) {
			case kafka.Error:
				if event.IsFatal() {
					return nil, event
				}
			case *kafka.Message:
				messages = append(messages, event)
			case kafka.PartitionEOF:
				// this is done as a hack to make sure that the log end offset
				// is committed whenever an offset reset happens [ only if auto.offset.reset = latest ]
				_, err := omc.kafCon.StoreOffsets([]kafka.TopicPartition{kafka.TopicPartition(event)})
				if err != nil {
					log.Printf("[partition EOF handler]: could not store offset for %v\n", event)
				}
			}

		}
		remainingTime = endTime.Sub(time.Now())
		if remainingTime < 0 {
			return messages, nil
		}
	}
	return messages, nil
}

func (omc *OffManConsumer) Consume(ctx context.Context, topics []string) error {

	err := omc.kafCon.SubscribeTopics(topics, makeRebalanceCB(omc.offMan))

	if err != nil {
		panic(err)
	}

	var consumeErr error
	run := true
	log.Printf("[Poll loop]: Starting consumer poll\n")
	for run {
		messages, err := omc.pollBatch(ctx, pollTimeoutMS)
		if err != nil {
			consumeErr = err
			log.Printf("[consumer poll]:error received from poll:%v\n", err)
			run = false
			break
		}
		if len(messages) > 0 {
			parts := toppar.MakePartitionMap(messages)
			omc.offMan.LoadPartitions(parts)

			partsToPause := make([]kafka.TopicPartition, 0, len(parts))
			for part, _ := range parts {
				partsToPause = append(partsToPause, toppar.TopicPartToKafkaTopicPart(part))
			}

			log.Printf("[consumer poll]: fetched %d records for %d parts", len(messages), len(parts))

			debug.Log("[consumer poll]: pausing partitions:%v", partsToPause)
			if err := omc.pauseParts(partsToPause); err != nil {
				consumeErr = err
				break
			}

			omc.handleRecords(ctx, messages)

		}

		if err := omc.commitOffsets(); err != nil {
			consumeErr = err
			break
		}
	}

	log.Printf("[consumer shutdown]: waiting for pending jobs to finish\n")
	omc.wg.Wait()

	log.Printf("[Consumer shutdown]:enqueueing remaining offsets to commit\n")
	if err := omc.commitOffsets(); err != nil {
		log.Printf("[Consumer shutdown]:offset commit error:%v\n", err)
	}

	if err := omc.kafCon.Close(); err != nil {
		log.Printf("[Consumer shutdown]:error closing consumer:%v\n", err)
	}

	log.Printf("[Consumer shutdown]: closing in-channel\n")
	close(omc.sendChan)

	log.Println("[Consumer shutdown]: Discarding future acknowledgements")
	log.Println("[Consumer shutdown]: shutdown complete")
	return consumeErr
}

func (omc *OffManConsumer) commitOffsets() error {

	committableMessages := omc.offMan.CommittableMessages()

	for _, committable := range committableMessages {
		debug.Log("[committable]: {tp = %s off = %d}", committable.TopPart, committable.Message.TopicPartition.Offset)
		_, err := omc.kafCon.StoreMessage(committable.Message)
		if err != nil {
			return err
		}
	}

	if err := omc.commitIfIntervalExceeded(); err != nil {
		return err
	}

	var finishedParts []kafka.TopicPartition
	for part, track := range committableMessages {
		if track.Committed {
			finishedParts = append(finishedParts, toppar.TopicPartToKafkaTopicPart(part))
		}
	}
	omc.offMan.ClearPartitions(toppar.KafkaTopPartsToTopParts(finishedParts))
	if err := omc.resumeParts(finishedParts); err != nil {
		return err
	}
	return nil
}

func (omc *OffManConsumer) commitIfIntervalExceeded() error {
	now := time.Now()
	timeDiff := now.Sub(omc.lastCommit).Milliseconds()

	if timeDiff >= int64(omc.commitIntervalMS) {
		log.Printf("[consumer Commit]: exceeded %dms by %dms, committing offsets\n", omc.commitIntervalMS, timeDiff-int64(omc.commitIntervalMS))

		committed, err := omc.kafCon.Commit()

		if err != nil && err.(kafka.Error).Code() != kafka.ErrNoOffset {
			return err
		}

		for _, topicPartition := range committed {
			topPar := toppar.KafkaTopicPartToTopicPart(topicPartition)
			if topicPartition.Offset != kafka.OffsetInvalid {
				if err := omc.offMan.MarkCommitCheckpoint(topPar, int64(topicPartition.Offset)); err != nil {
					log.Printf("[offman error]: offset checkpoint mark error:%s => %s  => %v\n", topPar, topicPartition.Offset, err)
				}
			}
		}

		omc.lastCommit = time.Now()
	}
	return nil
}

func (omc *OffManConsumer) pauseParts(parts []kafka.TopicPartition) error {
	if len(parts) > 0 {
		return omc.kafCon.Pause(parts)
	}
	return nil
}

func (omc *OffManConsumer) resumeParts(parts []kafka.TopicPartition) error {
	if len(parts) < 1 {
		return nil
	}

	log.Printf("[%s]resuming parts for %v\n", omc.kafCon.String(), parts)
	if err := omc.kafCon.Resume(parts); err != nil {
		return err
	}
	return nil
}

func (omc *OffManConsumer) Ack(m *kafka.Message) error {
	ktp := m.TopicPartition
	tp := toppar.KafkaTopicPartToTopicPart(ktp)
	return omc.offMan.Ack(tp, int64(ktp.Offset))
}

func (omc *OffManConsumer) handleRecords(ctx context.Context, messages []*kafka.Message) {
	omc.wg.Add(1)
	done := ctx.Done()
	go func() {
		defer omc.wg.Done()
		select {
		case <-done:
			return
		case omc.sendChan <- messages:
			debug.Log("[consumer send]: sent %d messages on in-channel\n", len(messages))
		}
	}()
}
