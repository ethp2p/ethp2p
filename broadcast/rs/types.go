package rs

import (
	"errors"

	"github.com/ethp2p/ethp2p/broadcast"
	rspb "github.com/ethp2p/ethp2p/broadcast/rs/pb"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	DataShards        int  `yaml:"data_shards"`
	ParityShards      int  `yaml:"parity_shards"`
	ChunkLen          int  `yaml:"chunk_len"`
	BitmapThreshold   int  `yaml:"bitmap_threshold"`
	ForwardMultiplier int  `yaml:"forward_multiplier"`
	DisableBitmap     bool `yaml:"disable_bitmap"`
}

func (c Config) Validate() error {
	if c.DataShards < 0 || c.ParityShards < 0 || c.ChunkLen < 0 || c.BitmapThreshold < 0 || c.ForwardMultiplier < 0 {
		return errors.New("RS config fields must be non-negative")
	}
	if c.ChunkLen == 0 && (c.DataShards == 0 || c.ParityShards == 0) {
		return errors.New("data_shards and parity_shards must be positive when chunk_len is zero")
	}
	return nil
}

func DefaultConfig() Config {
	return Config{
		DataShards:        16,
		ParityShards:      16,
		BitmapThreshold:   50,
		ForwardMultiplier: 4,
	}
}

// NewScheme returns a Scheme factory that creates unified sessionStrategy
// instances for the RS erasure code.
func NewScheme(config Config) broadcast.Scheme[*ChunkIdent, *Bitmap, *Preamble] {
	return broadcast.Scheme[*ChunkIdent, *Bitmap, *Preamble]{
		NewOrigin: func(_ broadcast.MessageID, payload []byte) (broadcast.Strategy[*ChunkIdent, *Bitmap], *Preamble, error) {
			s, err := newOriginStrategy(&config, payload)
			if err != nil {
				return nil, nil, err
			}
			p := s.preamble
			return s, &p, nil
		},
		NewRelay: func(_ broadcast.MessageID, p *Preamble) (broadcast.Strategy[*ChunkIdent, *Bitmap], error) {
			return newRelayStrategy(&config, p)
		},
		NewCI: func() *ChunkIdent { return &ChunkIdent{} },
		NewR:  func() *Bitmap { b := Bitmap{}; return &b },
		NewP:  func() *Preamble { return &Preamble{} },
	}
}

type ChunkIdent struct {
	Index int
}

func (ci ChunkIdent) Marshal() ([]byte, error) {
	return proto.Marshal(&rspb.ChunkIdent{
		Index: int32(ci.Index),
	})
}

func (ci *ChunkIdent) Unmarshal(data []byte) error {
	msg := &rspb.ChunkIdent{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return err
	}
	ci.Index = int(msg.Index)
	return nil
}

func (ci *ChunkIdent) Handle() broadcast.ChunkHandle {
	return broadcast.ChunkHandle(ci.Index)
}

type Preamble struct {
	DataChunks    int
	ParityChunks  int
	MessageLength int
	// TODO dependent on auth scheme
	ChunkHashes [][]byte
	// TODO dependent on auth scheme
	MessageHash []byte
}

func (p Preamble) TotalChunks() int {
	return p.DataChunks + p.ParityChunks
}

func (p Preamble) Marshal() ([]byte, error) {
	return proto.Marshal(&rspb.Preamble{
		NumData:   int32(p.DataChunks),
		NumParity: int32(p.ParityChunks),
		Length:    int32(p.MessageLength),
		Hashes:    p.ChunkHashes,
		Hash:      p.MessageHash,
	})
}

func (p *Preamble) Unmarshal(data []byte) error {
	pb := &rspb.Preamble{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return err
	}
	p.DataChunks = int(pb.NumData)
	p.ParityChunks = int(pb.NumParity)
	p.MessageLength = int(pb.Length)
	p.ChunkHashes = pb.Hashes
	p.MessageHash = pb.Hash
	return nil
}

func initPreamble(cfg *Config, msgLen int) (preamble Preamble, chunkLen int) {
	preamble = Preamble{MessageLength: msgLen}
	chunkLen = cfg.ChunkLen

	if chunkLen == 0 {
		preamble.DataChunks = cfg.DataShards
		preamble.ParityChunks = cfg.ParityShards
		chunkLen = divCeil(msgLen, preamble.DataChunks)
		// The leopard-RS algorithm (used when shard count >= 256)
		// operates on 64-byte blocks internally, so chunk lengths
		// must be a multiple of 64 for correct encoding.
		if preamble.TotalChunks() >= 256 {
			chunkLen += (64 - chunkLen%64) % 64
		}
	} else {
		preamble.DataChunks = divCeil(msgLen, chunkLen)
		preamble.ParityChunks = preamble.DataChunks // 2x
	}

	return preamble, chunkLen
}
