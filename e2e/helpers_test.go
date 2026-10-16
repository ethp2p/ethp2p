package e2e

import (
	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/broadcast/rs"
	"github.com/ethp2p/ethp2p/internal/harness"
)

func payload(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// fixedRS uses a fixed number of shards, so send-count scenarios have an
// exact network oracle rather than a payload-size-dependent shard count.
func fixedRS(nd *harness.Node, id broadcast.ChannelID, dataShards, parityShards int) *harness.Channel {
	config := rs.DefaultConfig()
	config.DataShards = dataShards
	config.ParityShards = parityShards
	return harness.Attach(nd, id, rs.NewScheme(config))
}
