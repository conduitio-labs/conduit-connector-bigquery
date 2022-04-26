// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package googlesource

import (
	"context"
	"errors"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	bqStoragepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1"
	"gopkg.in/tomb.v2"
)

type Source struct {
	sdk.UnimplementedSource
	Session         *bqStoragepb.ReadSession
	BQReadClient    *bigquery.Client
	SourceConfig    googlebigquery.SourceConfig
	Tables          []string
	Ctx             context.Context
	SDKResponse     chan sdk.Record
	LatestPositions latestPositions
	Position        Position
	ticker          *time.Ticker
	tomb            *tomb.Tomb
}

type latestPositions struct {
	LatestPositions map[string]Position
	lock            sync.Mutex
}

type Position struct {
	TableID string
	Offset  int
}

func NewSource() sdk.Source {
	return &Source{}
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Trace().Msg("Configuring a Source Connector.")
	sourceConfig, err := googlebigquery.ParseSourceConfig(cfg)
	if err != nil {
		sdk.Logger(ctx).Error().Str("err", err.Error()).Msg("blank config provided")
		return err
	}

	s.SourceConfig = sourceConfig
	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) (err error) {

	s.Ctx = ctx
	s.Position = fetchPos(s, pos)

	// s.SDKResponse is a buffered channel that contains records
	//  coming from all the tables which user wants to sync.
	s.SDKResponse = make(chan sdk.Record, 100)
	s.ticker = time.NewTicker(googlebigquery.PollingTime)
	s.tomb = &tomb.Tomb{}

	s.LatestPositions.lock.Lock()
	s.LatestPositions.LatestPositions = make(map[string]Position)
	s.LatestPositions.lock.Unlock()

	s.tomb.Go(s.runIterator)
	sdk.Logger(ctx).Trace().Msg("end of function: open")
	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {

	sdk.Logger(ctx).Trace().Msg("Stated read function")
	var response sdk.Record

	response, err := s.Next(s.Ctx)
	if err != nil {
		sdk.Logger(ctx).Trace().Str("err", err.Error()).Msg("Error from endpoint.")
		return sdk.Record{}, err
	}

	return response, nil

}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {

	if s.SDKResponse != nil {
		close(s.SDKResponse)
	}
	err := s.StopIterator()
	if err != nil {
		sdk.Logger(s.Ctx).Error().Str("err", string(err.Error())).Msg("got error while closing bigquery client")
		return err
	}
	return nil
}

func (s *Source) StopIterator() error {
	if s.BQReadClient != nil {
		err := s.BQReadClient.Close()
		if err != nil {
			sdk.Logger(s.Ctx).Error().Str("err", string(err.Error())).Msg("got error while closing bigquery client")
			return err
		}
	}
	s.ticker.Stop()
	s.tomb.Kill(errors.New("iterator is stopped"))

	return nil
}
