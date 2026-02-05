package pkg

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"
)

// BrokerInfo contains information about a Kafka broker and its topics/partitions
type BrokerInfo struct {
	BrokerID  int              `json:"brokerId"`
	Address   string           `json:"address"`
	Reachable bool             `json:"reachable"`
	Topics    map[string][]int `json:"topics"`
}

// ClusterInfo wraps the broker information
type ClusterInfo struct {
	Brokers []BrokerInfo `json:"brokers"`
}

// InfoConfig contains configuration for collecting cluster information
type InfoConfig struct {
	Brokers []string
}

// CollectInfo collects information about the Kafka cluster
// Returns ClusterInfo containing a slice of BrokerInfo, one per broker, with their topics and partitions
func CollectInfo(ctx context.Context, cfg InfoConfig) (*ClusterInfo, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("at least one broker address is required")
	}

	// Connect to any broker to get metadata
	var conn *kafka.Conn
	var err error
	for _, broker := range cfg.Brokers {
		conn, err = kafka.DialContext(ctx, "tcp", broker)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to any broker (tried: %v): %w", cfg.Brokers, err)
	}
	defer conn.Close()

	// Get broker list
	brokers, err := conn.Brokers()
	if err != nil {
		return nil, fmt.Errorf("failed to get broker list: %w", err)
	}

	// Get partitions for all topics (empty slice means all topics)
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}

	// Build broker info map
	brokerMap := make(map[int]*BrokerInfo)
	for _, broker := range brokers {
		brokerAddress := broker.Host + ":" + fmt.Sprintf("%d", broker.Port)
		brokerMap[broker.ID] = &BrokerInfo{
			BrokerID:  broker.ID,
			Address:   brokerAddress,
			Reachable: isBrokerReachable(ctx, brokerAddress),
			Topics:    make(map[string][]int),
		}
	}

	// Process partitions and assign them to brokers
	for _, partition := range partitions {
		// Add partition to all replica brokers (including leader)
		for _, replica := range partition.Replicas {
			brokerInfo := brokerMap[replica.ID]
			if brokerInfo == nil {
				// Broker not in broker list, skip
				continue
			}

			// Add partition to this broker's topic
			if brokerInfo.Topics[partition.Topic] == nil {
				brokerInfo.Topics[partition.Topic] = make([]int, 0)
			}
			brokerInfo.Topics[partition.Topic] = append(brokerInfo.Topics[partition.Topic], partition.ID)
		}
	}

	// Sort broker IDs and partition IDs for consistent output
	brokerIDs := make([]int, 0, len(brokerMap))
	for id := range brokerMap {
		brokerIDs = append(brokerIDs, id)
	}
	sort.Ints(brokerIDs)

	result := make([]BrokerInfo, 0, len(brokerMap))
	for _, id := range brokerIDs {
		brokerInfo := brokerMap[id]
		// Sort partition IDs for each topic
		for topicName := range brokerInfo.Topics {
			sort.Ints(brokerInfo.Topics[topicName])
		}
		result = append(result, *brokerInfo)
	}

	return &ClusterInfo{
		Brokers: result,
	}, nil
}

// isBrokerReachable checks if a broker is reachable by attempting to connect to it
func isBrokerReachable(ctx context.Context, address string) bool {
	// Create a context with a short timeout for reachability check
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	conn, err := kafka.DialContext(checkCtx, "tcp", address)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
