package kafka

import (
	"context"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Conn represents a connection to a Kafka broker
type Conn struct {
	conn *kafkago.Conn
}

// Broker represents a Kafka broker
type Broker struct {
	ID      int
	Host    string
	Port    int
	Address string
}

// Partition represents a Kafka partition
type Partition struct {
	Topic     string
	ID        int
	Leader    Broker
	Replicas  []Broker
	Isr       []Broker
}

// ConnectToAnyBroker connects to the first available broker from the given list
func ConnectToAnyBroker(ctx context.Context, brokers []string) (*Conn, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("at least one broker address is required")
	}

	var conn *kafkago.Conn
	var err error
	for _, broker := range brokers {
		conn, err = kafkago.DialContext(ctx, "tcp", broker)
		if err == nil {
			return &Conn{conn: conn}, nil
		}
	}
	return nil, fmt.Errorf("failed to connect to any broker (tried: %v): %w", brokers, err)
}

// Close closes the connection
func (c *Conn) Close() error {
	return c.conn.Close()
}

// GetBrokerList retrieves the list of all brokers from the cluster
func GetBrokerList(conn *Conn) ([]Broker, error) {
	brokers, err := conn.conn.Brokers()
	if err != nil {
		return nil, fmt.Errorf("failed to get broker list: %w", err)
	}
	
	result := make([]Broker, 0, len(brokers))
	for _, b := range brokers {
		result = append(result, Broker{
			ID:      b.ID,
			Host:    b.Host,
			Port:    b.Port,
			Address: FormatBrokerAddressFromKafkaGo(b),
		})
	}
	return result, nil
}

// FormatBrokerAddress formats a broker's host and port into an address string
func FormatBrokerAddress(broker Broker) string {
	return broker.Address
}

// FormatBrokerAddressFromKafkaGo formats a kafka-go broker's host and port into an address string
func FormatBrokerAddressFromKafkaGo(broker kafkago.Broker) string {
	return fmt.Sprintf("%s:%d", broker.Host, broker.Port)
}

// CreateBrokerIDToAddressMap creates a map from broker ID to broker address
func CreateBrokerIDToAddressMap(brokers []Broker) map[int]string {
	brokerMap := make(map[int]string, len(brokers))
	for _, broker := range brokers {
		brokerMap[broker.ID] = broker.Address
	}
	return brokerMap
}

// ReadAllPartitions reads all partition metadata from the cluster
func ReadAllPartitions(conn *Conn) ([]Partition, error) {
	partitions, err := conn.conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}
	
	result := make([]Partition, 0, len(partitions))
	for _, p := range partitions {
		partition := Partition{
			Topic:  p.Topic,
			ID:     p.ID,
			Leader: Broker{
				ID:      p.Leader.ID,
				Host:    p.Leader.Host,
				Port:    p.Leader.Port,
				Address: FormatBrokerAddressFromKafkaGo(p.Leader),
			},
			Replicas: make([]Broker, 0, len(p.Replicas)),
			Isr:      make([]Broker, 0, len(p.Isr)),
		}
		
		for _, r := range p.Replicas {
			partition.Replicas = append(partition.Replicas, Broker{
				ID:      r.ID,
				Host:    r.Host,
				Port:    r.Port,
				Address: FormatBrokerAddressFromKafkaGo(r),
			})
		}
		
		for _, isr := range p.Isr {
			partition.Isr = append(partition.Isr, Broker{
				ID:      isr.ID,
				Host:    isr.Host,
				Port:    isr.Port,
				Address: FormatBrokerAddressFromKafkaGo(isr),
			})
		}
		
		result = append(result, partition)
	}
	return result, nil
}

// DialLeader connects to the leader broker for a specific topic-partition
func DialLeader(ctx context.Context, network, address, topic string, partitionID int) (*Conn, error) {
	conn, err := kafkago.DialLeader(ctx, network, address, topic, partitionID)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: conn}, nil
}

// ReadOffsets reads the earliest and latest offsets for a partition
func (c *Conn) ReadOffsets() (int64, int64, error) {
	return c.conn.ReadOffsets()
}

// IsBrokerReachable checks if a broker is reachable by attempting to connect to it
func IsBrokerReachable(ctx context.Context, address string) bool {
	// Create a context with a short timeout for reachability check
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	conn, err := kafkago.DialContext(checkCtx, "tcp", address)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
