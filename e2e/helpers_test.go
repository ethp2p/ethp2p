package e2e

import (
	"github.com/ethp2p/ethp2p/broadcast"
	"github.com/ethp2p/ethp2p/broadcast/rs"
	"github.com/ethp2p/ethp2p/internal/nettest"
)

func payload(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func smallRS(nd *nettest.Node, id broadcast.ChannelID) *nettest.Channel {
	config := rs.DefaultConfig()
	config.DataShards = 4
	config.ParityShards = 4
	config.ChunkLen = 1024
	return nettest.Attach(nd, id, rs.NewScheme(config))
}

// fixedRS uses a fixed number of shards, so send-count scenarios have an
// exact network oracle rather than a payload-size-dependent shard count.
func fixedRS(nd *nettest.Node, id broadcast.ChannelID, dataShards, parityShards int) *nettest.Channel {
	config := rs.DefaultConfig()
	config.DataShards = dataShards
	config.ParityShards = parityShards
	return nettest.Attach(nd, id, rs.NewScheme(config))
}
