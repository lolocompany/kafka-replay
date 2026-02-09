package admin

import (
	"context"
	"fmt"
	"net"
	"sort"

	"github.com/segmentio/kafka-go"
)

// ConsumerGroupInfo contains information about a consumer group
type ConsumerGroupInfo struct {
	GroupID      string
	State        string
	ProtocolType string
	Members      []MemberInfo
	Offsets      []OffsetInfo
}

// MemberInfo contains information about a consumer group member
type MemberInfo struct {
	MemberID       string
	ClientID       string
	ClientHost     string
	AssignedTopics map[string][]int // topic -> partition IDs
}

// OffsetInfo contains offset information for a topic-partition
type OffsetInfo struct {
	Topic     string
	Partition int
	Offset    int64
	Metadata  string
}

// listConsumerGroups lists all consumer groups using the Client API
func listConsumerGroups(ctx context.Context, client *kafka.Client, brokerAddr net.Addr) ([]string, error) {
	req := &kafka.ListGroupsRequest{
		Addr: brokerAddr,
	}

	resp, err := client.ListGroups(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}

	if resp.Error != nil {
		return nil, fmt.Errorf("error listing consumer groups: %w", resp.Error)
	}

	groups := make([]string, 0, len(resp.Groups))
	for _, group := range resp.Groups {
		// Only include consumer groups (protocol type "consumer")
		if group.ProtocolType == "consumer" {
			groups = append(groups, group.GroupID)
		}
	}

	sort.Strings(groups)
	return groups, nil
}

// findGroupCoordinator finds the coordinator broker using the Client API
func findGroupCoordinator(ctx context.Context, client *kafka.Client, brokerAddr net.Addr, groupID string) (*kafka.Broker, error) {
	req := &kafka.FindCoordinatorRequest{
		Key:     groupID,
		KeyType: kafka.CoordinatorKeyTypeConsumer,
		Addr:    brokerAddr,
	}

	resp, err := client.FindCoordinator(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to find group coordinator: %w", err)
	}

	if resp.Error != nil {
		return nil, fmt.Errorf("error finding coordinator: %w", resp.Error)
	}

	if resp.Coordinator == nil {
		return nil, fmt.Errorf("coordinator not found in response")
	}

	// Convert FindCoordinatorResponseCoordinator to Broker
	return &kafka.Broker{
		ID:   resp.Coordinator.NodeID,
		Host: resp.Coordinator.Host,
		Port: resp.Coordinator.Port,
	}, nil
}

// describeConsumerGroup describes a consumer group using the Client API
func describeConsumerGroup(ctx context.Context, client *kafka.Client, brokerAddr net.Addr, groupID string) (*ConsumerGroupInfo, error) {
	req := &kafka.DescribeGroupsRequest{
		GroupIDs: []string{groupID},
		Addr:     brokerAddr,
	}

	resp, err := client.DescribeGroups(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to describe consumer group: %w", err)
	}

	if len(resp.Groups) == 0 {
		return nil, fmt.Errorf("consumer group %s not found", groupID)
	}

	groupDesc := resp.Groups[0]
	if groupDesc.Error != nil {
		return nil, fmt.Errorf("error describing group: %w", groupDesc.Error)
	}

	info := &ConsumerGroupInfo{
		GroupID:      groupDesc.GroupID,
		State:        groupDesc.GroupState,
		ProtocolType: "consumer", // Default to consumer, could be fetched from ListGroups if needed
		Members:      make([]MemberInfo, 0, len(groupDesc.Members)),
	}

	// Parse member information
	for _, member := range groupDesc.Members {
		memberInfo := MemberInfo{
			MemberID:       member.MemberID,
			ClientID:       member.ClientID,
			ClientHost:     member.ClientHost,
			AssignedTopics: make(map[string][]int),
		}

		// Populate assigned topics from member assignments
		for _, topic := range member.MemberAssignments.Topics {
			memberInfo.AssignedTopics[topic.Topic] = topic.Partitions
		}

		info.Members = append(info.Members, memberInfo)
	}

	return info, nil
}

// getConsumerGroupOffsets gets offset information using the Client API
func getConsumerGroupOffsets(ctx context.Context, client *kafka.Client, brokerAddr net.Addr, groupID string) ([]OffsetInfo, error) {
	req := &kafka.OffsetFetchRequest{
		GroupID: groupID,
		Topics:  nil, // nil means fetch all topics
		Addr:    brokerAddr,
	}

	resp, err := client.OffsetFetch(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch offsets: %w", err)
	}

	offsets := make([]OffsetInfo, 0)
	for topic, partitions := range resp.Topics {
		for _, partitionInfo := range partitions {
			offsets = append(offsets, OffsetInfo{
				Topic:     topic,
				Partition: partitionInfo.Partition,
				Offset:    partitionInfo.CommittedOffset,
				Metadata:  partitionInfo.Metadata,
			})
		}
	}

	// Sort by topic and partition
	sort.Slice(offsets, func(i, j int) bool {
		if offsets[i].Topic != offsets[j].Topic {
			return offsets[i].Topic < offsets[j].Topic
		}
		return offsets[i].Partition < offsets[j].Partition
	})

	return offsets, nil
}

// ListConsumerGroups lists all consumer groups in the cluster
func ListConsumerGroups(ctx context.Context, brokers []string) ([]string, error) {
	// Create a client - we'll use the first broker address
	if len(brokers) == 0 {
		return nil, fmt.Errorf("at least one broker address is required")
	}

	brokerAddr := kafka.TCP(brokers[0])
	client := &kafka.Client{
		Addr: brokerAddr,
	}

	return listConsumerGroups(ctx, client, brokerAddr)
}

// DescribeConsumerGroup describes a specific consumer group
func DescribeConsumerGroup(ctx context.Context, brokers []string, groupID string, includeOffsets bool, includeMembers bool) (*ConsumerGroupInfo, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("at least one broker address is required")
	}

	// Create a client using the first broker
	brokerAddr := kafka.TCP(brokers[0])
	client := &kafka.Client{
		Addr: brokerAddr,
	}

	// Find the group coordinator
	coordinator, err := findGroupCoordinator(ctx, client, brokerAddr, groupID)
	if err != nil {
		return nil, err
	}

	// Create coordinator address
	coordinatorAddr := kafka.TCP(fmt.Sprintf("%s:%d", coordinator.Host, coordinator.Port))

	// Describe the group
	info, err := describeConsumerGroup(ctx, client, coordinatorAddr, groupID)
	if err != nil {
		return nil, err
	}

	// Fetch offsets if requested
	if includeOffsets {
		offsets, err := getConsumerGroupOffsets(ctx, client, coordinatorAddr, groupID)
		if err != nil {
			// Don't fail if offsets can't be fetched, just log it
			info.Offsets = []OffsetInfo{}
		} else {
			info.Offsets = offsets
		}
	}

	return info, nil
}
