package main

import (
	"encoding/json"
	"fmt"
	"image"
	"io"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	
	"github.com/bluenviron/gortsplib/v4"
	"github.com/bluenviron/gortsplib/v4/pkg/base"
	"github.com/bluenviron/gortsplib/v4/pkg/format"
	"github.com/bluenviron/gortsplib/v4/pkg/format/rtph265"
	"github.com/pion/rtp"
)

//Hooks that may be overridden for testing
var inputReader io.Reader = os.Stdin
var outputWriter io.Writer = os.Stdout

// Instantiate a producer
var producer sarama.AsyncProducer

func main() {
	//Sarama logger
	sarama.Logger = log.New(outputWriter, "[saramaLog]", log.Ltime)

	//Create a Kafka producer
	var brokers = []string{os.Getenv("KAFKAPORT")}
	var err error
	producer, err = kafkapc.CreateKafkaProducer(brokers)
	if err != nil {
		panic("Failed to connect to Kafka. Error: " + err.Error())
	}
	//Close producer to flush(i.e., push) all batched messages into Kafka queue
	defer func() { producer.Close() }()

	c := gortsplib.Client{}

	// parse URL
	u, err := base.ParseURL("rtsp://laurettatraining:8554/172-16-17-55")
	if err != nil {
		panic(err)
	}

	// connect to the server
	err = c.Start(u.Scheme, u.Host)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	// find available medias
	desc, _, err := c.Describe(u)
	if err != nil {
		panic(err)
	}

	// find the H265 media and format
	var forma *format.H265
	medi := desc.FindFormat(&forma)
	if medi == nil {
		panic("media not found")
	}

	// setup RTP/H265 -> H265 decoder
	rtpDec, err := forma.CreateDecoder()
	if err != nil {
		panic(err)
	}

	// setup H265 -> raw frames decoder
	frameDec := &h265Decoder{}
	err = frameDec.initialize()
	if err != nil {
		panic(err)
	}
	defer frameDec.close()

	// if VPS, SPS and PPS are present into the SDP, send them to the decoder
	if forma.VPS != nil {
		frameDec.decode(forma.VPS)
	}
	if forma.SPS != nil {
		frameDec.decode(forma.SPS)
	}
	if forma.PPS != nil {
		frameDec.decode(forma.PPS)
	}

	// setup a single media
	_, err = c.Setup(desc.BaseURL, medi, 0, 0)
	if err != nil {
		panic(err)
	}

	// called when a RTP packet arrives
	c.OnPacketRTP(medi, forma, func(pkt *rtp.Packet) {
		// decode timestamp
		pts, ok := c.PacketPTS(medi, pkt)
		if !ok {
			log.Printf("waiting for timestamp")
			return
		}

		// extract access units from RTP packets
		au, err := rtpDec.Decode(pkt)
		if err != nil {
			if err != rtph265.ErrNonStartingPacketAndNoPrevious && err != rtph265.ErrMorePacketsNeeded {
				log.Printf("ERR: %v", err)
			}
			return
		}

		for _, nalu := range au {
			// convert NALUs into RGBA frames
			img, err := frameDec.decode(nalu)
			if err != nil {
				panic(err)
			}

			// wait for a frame
			if img == nil {
				continue
			}

			

			log.Printf("decoded frame with PTS %v and size %v", pts, img.Bounds().Max)
		}
	})

	msg := &sarama.ProducerMessage{
		Topic:     os.Getenv("TOPICNAME"),
		Value:     sarama.ByteEncoder(img),
		Timestamp: time.Now(),
	}
	//Send message into Kafka queue
	producer.Input() <- msg

	//Print time of receiving each image to show the code is running
	fmt.Fprintf(outputWriter, "---->>>> %v\n", time.Now())

	// wait until a fatal error
	panic(c.Wait())
}

//Result represents the Kafka queue message format
type Result struct {
	Pix      []byte `json:"pix"`
	Channels int    `json:"channels"`
	Rows     int    `json:"rows"`
	Cols     int    `json:"cols"`
}
