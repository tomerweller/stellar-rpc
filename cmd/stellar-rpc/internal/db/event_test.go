package db

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
)

func transactionMetaWithEvents(events ...xdr.ContractEvent) xdr.TransactionMeta {
	// Invent some pre- and post-apply events.
	stages := []xdr.TransactionEventStage{
		xdr.TransactionEventStageTransactionEventStageAfterAllTxs,
		xdr.TransactionEventStageTransactionEventStageBeforeAllTxs,
		xdr.TransactionEventStageTransactionEventStageAfterTx,
	}
	body := xdr.ContractEventV0{
		Data:   xdr.ScVal{Type: xdr.ScValTypeScvVoid},
		Topics: []xdr.ScVal{{Type: xdr.ScValTypeScvVoid}},
	}

	txEvents := []xdr.TransactionEvent{}
	for _, stage := range stages {
		txEvents = append(txEvents, xdr.TransactionEvent{
			Stage: stage,
			Event: xdr.ContractEvent{
				Type: xdr.ContractEventTypeSystem,
				Body: xdr.ContractEventBody{
					V:  0,
					V0: &body,
				},
			},
		})
	}

	return xdr.TransactionMeta{
		V:          4,
		Operations: &[]xdr.OperationMeta{},
		V4: &xdr.TransactionMetaV4{
			Events: txEvents,
			Operations: []xdr.OperationMetaV2{
				{Events: events},
			},
		},
	}
}

func contractEvent(contractID xdr.ContractId, topic []xdr.ScVal, body xdr.ScVal) xdr.ContractEvent {
	return xdr.ContractEvent{
		ContractId: &contractID,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: topic,
				Data:   body,
			},
		},
	}
}

func ledgerCloseMetaWithEvents(
	sequence uint32,
	closeTimestamp int64,
	txMeta ...xdr.TransactionMeta,
) xdr.LedgerCloseMeta {
	txProcessing := make([]xdr.TransactionResultMeta, 0, len(txMeta))
	phases := make([]xdr.TransactionPhase, 0, len(txMeta))

	for _, item := range txMeta {
		var operations []xdr.Operation
		for range item.MustV4().Operations {
			operations = append(operations,
				xdr.Operation{
					Body: xdr.OperationBody{
						Type: xdr.OperationTypeInvokeHostFunction,
						InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
							HostFunction: xdr.HostFunction{
								Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
								InvokeContract: &xdr.InvokeContractArgs{
									ContractAddress: xdr.ScAddress{
										Type:       xdr.ScAddressTypeScAddressTypeContract,
										ContractId: &xdr.ContractId{0x1, 0x2},
									},
									FunctionName: "foo",
									Args:         nil,
								},
							},
							Auth: []xdr.SorobanAuthorizationEntry{},
						},
					},
				})
		}
		envelope := xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					Ext:           xdr.TransactionExt{V: 1, SorobanData: &xdr.SorobanTransactionData{}},
					SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
					Operations:    operations,
				},
			},
		}
		txHash, err := network.HashTransactionInEnvelope(envelope, network.FutureNetworkPassphrase)
		if err != nil {
			panic(err)
		}

		txProcessing = append(txProcessing, xdr.TransactionResultMeta{
			TxApplyProcessing: item,
			Result: xdr.TransactionResultPair{
				TransactionHash: txHash,
			},
		})
		phases = append(phases, xdr.TransactionPhase{
			V: 0,
			V0Components: &[]xdr.TxSetComponent{
				{
					Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
					TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
						Txs: []xdr.TransactionEnvelope{
							envelope,
						},
					},
				},
			},
		})
	}

	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Hash: xdr.Hash{},
				Header: xdr.LedgerHeader{
					ScpValue: xdr.StellarValue{
						CloseTime: xdr.TimePoint(closeTimestamp),
					},
					LedgerSeq: xdr.Uint32(sequence),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					PreviousLedgerHash: xdr.Hash{},
					Phases:             phases,
				},
			},
			TxProcessing: txProcessing,
		},
	}
}

func TestInsertEvents(t *testing.T) {
	db := NewTestDB(t)
	ctx := context.TODO()
	log := log.DefaultLogger
	log.SetLevel(logrus.TraceLevel)
	now := time.Now().UTC()

	writer := NewReadWriter(log, db, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(t, err)
	contractID := xdr.ContractId([32]byte{})
	counter := xdr.ScSymbol("COUNTER")

	txMeta := make([]xdr.TransactionMeta, 0, 10)
	for range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} {
		txMeta = append(txMeta, transactionMetaWithEvents(
			contractEvent(
				contractID,
				xdr.ScVec{xdr.ScVal{
					Type: xdr.ScValTypeScvSymbol,
					Sym:  &counter,
				}},
				xdr.ScVal{
					Type: xdr.ScValTypeScvSymbol,
					Sym:  &counter,
				},
			),
		))
	}
	ledgerCloseMeta := ledgerCloseMetaWithEvents(1, now.Unix(), txMeta...)

	eventW := write.EventWriter()
	err = eventW.InsertEvents(ledgerCloseMeta)
	require.NoError(t, err)

	eventReader := NewEventReader(log, db, passphrase)
	start := protocol.Cursor{Ledger: 1}
	end := protocol.Cursor{Ledger: 100}
	cursorRange := protocol.CursorRange{Start: start, End: end}

	// Pass nil for filterTopics to get all events
	err = eventReader.GetEvents(ctx, cursorRange, nil, nil, nil, EventOrderAsc, nil)
	require.NoError(t, err)
}

// TestGetEventsSingleFilterANDLogic verifies that within a single filter,
// topic positions use AND logic - an event must match ALL specified positions.
func TestGetEventsSingleFilterANDLogic(t *testing.T) {
	db := NewTestDB(t)
	ctx := context.TODO()
	logger := log.DefaultLogger
	logger.SetLevel(logrus.TraceLevel)
	now := time.Now().UTC()

	writer := NewReadWriter(logger, db, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(t, err)

	// Create distinct topic values
	transfer := xdr.ScSymbol("transfer")
	mint := xdr.ScSymbol("mint")
	addressA := xdr.ScSymbol("addressA")
	addressB := xdr.ScSymbol("addressB")

	transferTopic := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &transfer}
	mintTopic := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &mint}
	addressATopic := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &addressA}
	addressBTopic := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &addressB}

	contractID := xdr.ContractId([32]byte{1, 2, 3})
	dataVal := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &transfer}

	// Create 4 events with different topic combinations:
	// Event 1: topic1=transfer, topic2=addressA
	// Event 2: topic1=transfer, topic2=addressB
	// Event 3: topic1=mint, topic2=addressA
	// Event 4: topic1=mint, topic2=addressB

	txMeta := []xdr.TransactionMeta{
		transactionMetaWithEvents(contractEvent(contractID, []xdr.ScVal{transferTopic, addressATopic}, dataVal)),
		transactionMetaWithEvents(contractEvent(contractID, []xdr.ScVal{transferTopic, addressBTopic}, dataVal)),
		transactionMetaWithEvents(contractEvent(contractID, []xdr.ScVal{mintTopic, addressATopic}, dataVal)),
		transactionMetaWithEvents(contractEvent(contractID, []xdr.ScVal{mintTopic, addressBTopic}, dataVal)),
	}

	ledgerCloseMeta := ledgerCloseMetaWithEvents(1, now.Unix(), txMeta...)

	eventW := write.EventWriter()
	err = eventW.InsertEvents(ledgerCloseMeta)
	require.NoError(t, err)
	require.NoError(t, write.Commit(ledgerCloseMeta, nil))

	eventReader := NewEventReader(logger, db, passphrase)
	cursorRange := protocol.CursorRange{
		Start: protocol.Cursor{Ledger: 1},
		End:   protocol.Cursor{Ledger: 100},
	}

	// Encode topic values for querying
	transferBytes, err := transferTopic.MarshalBinary()
	require.NoError(t, err)
	addressABytes, err := addressATopic.MarshalBinary()
	require.NoError(t, err)

	var foundEvents []protocol.Cursor
	scanFunc := func(event xdr.DiagnosticEvent, cursor protocol.Cursor, ledgerCloseTimestamp int64, txHash *xdr.Hash) bool {
		foundEvents = append(foundEvents, cursor)
		return true // continue scanning
	}

	// Test 1: Query with topic1=transfer AND topic2=addressA (single filter)
	// Should return exactly 1 event
	filterTopics := AllFilterTopics{
		FilterTopics{
			TopicFilter{Positions: [4][]byte{transferBytes, addressABytes, nil, nil}},
		},
	}
	err = eventReader.GetEvents(ctx, cursorRange, nil, filterTopics, nil, EventOrderAsc, scanFunc)
	require.NoError(t, err)
	require.Len(t, foundEvents, 1, "Expected exactly 1 event matching topic1=transfer AND topic2=addressA")

	// Test 2: Query with topic1=transfer only (topic2=wildcard)
	// Should return 2 events (transfer+addressA and transfer+addressB)
	foundEvents = nil
	filterTopics = AllFilterTopics{
		FilterTopics{
			TopicFilter{Positions: [4][]byte{transferBytes, nil, nil, nil}},
		},
	}
	err = eventReader.GetEvents(ctx, cursorRange, nil, filterTopics, nil, EventOrderAsc, scanFunc)
	require.NoError(t, err)
	require.Len(t, foundEvents, 2, "Expected 2 events matching topic1=transfer")

	// Test 3: Query with topic2=addressA only (topic1=wildcard)
	// Should return 2 events (transfer+addressA and mint+addressA)
	foundEvents = nil
	filterTopics = AllFilterTopics{
		FilterTopics{
			TopicFilter{Positions: [4][]byte{nil, addressABytes, nil, nil}},
		},
	}
	err = eventReader.GetEvents(ctx, cursorRange, nil, filterTopics, nil, EventOrderAsc, scanFunc)
	require.NoError(t, err)
	require.Len(t, foundEvents, 2, "Expected 2 events matching topic2=addressA")
}

// TestGetEventsMultiFilterORLogic verifies that multiple filters use OR logic
// between them - an event matches if it matches ANY filter.
// This is the critical test for the "transfers to OR from address" use case.
func TestGetEventsMultiFilterORLogic(t *testing.T) {
	db := NewTestDB(t)
	ctx := context.TODO()
	logger := log.DefaultLogger
	logger.SetLevel(logrus.TraceLevel)
	now := time.Now().UTC()

	writer := NewReadWriter(logger, db, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(t, err)

	// Create distinct topic values
	transfer := xdr.ScSymbol("transfer")
	addressA := xdr.ScSymbol("addressA")
	addressB := xdr.ScSymbol("addressB")
	addressC := xdr.ScSymbol("addressC")

	transferTopic := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &transfer}
	addressATopic := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &addressA}
	addressBTopic := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &addressB}
	addressCTopic := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &addressC}

	contractID := xdr.ContractId([32]byte{1, 2, 3})
	dataVal := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &transfer}

	// Create events simulating transfers:
	// Transfer events have: [transfer, from_address, to_address]
	// Event 1: transfer from A to B (should match "transfers involving A")
	// Event 2: transfer from B to A (should match "transfers involving A")
	// Event 3: transfer from B to C (should NOT match "transfers involving A")
	// Event 4: transfer from C to B (should NOT match "transfers involving A")

	txMeta := []xdr.TransactionMeta{
		transactionMetaWithEvents(contractEvent(contractID, []xdr.ScVal{transferTopic, addressATopic, addressBTopic}, dataVal)), // A -> B
		transactionMetaWithEvents(contractEvent(contractID, []xdr.ScVal{transferTopic, addressBTopic, addressATopic}, dataVal)), // B -> A
		transactionMetaWithEvents(contractEvent(contractID, []xdr.ScVal{transferTopic, addressBTopic, addressCTopic}, dataVal)), // B -> C
		transactionMetaWithEvents(contractEvent(contractID, []xdr.ScVal{transferTopic, addressCTopic, addressBTopic}, dataVal)), // C -> B
	}

	ledgerCloseMeta := ledgerCloseMetaWithEvents(1, now.Unix(), txMeta...)

	eventW := write.EventWriter()
	err = eventW.InsertEvents(ledgerCloseMeta)
	require.NoError(t, err)
	require.NoError(t, write.Commit(ledgerCloseMeta, nil))

	eventReader := NewEventReader(logger, db, passphrase)
	cursorRange := protocol.CursorRange{
		Start: protocol.Cursor{Ledger: 1},
		End:   protocol.Cursor{Ledger: 100},
	}

	// Encode topic values for querying
	transferBytes, err := transferTopic.MarshalBinary()
	require.NoError(t, err)
	addressABytes, err := addressATopic.MarshalBinary()
	require.NoError(t, err)

	var foundEvents []protocol.Cursor
	scanFunc := func(event xdr.DiagnosticEvent, cursor protocol.Cursor, ledgerCloseTimestamp int64, txHash *xdr.Hash) bool {
		foundEvents = append(foundEvents, cursor)
		return true // continue scanning
	}

	// Query: "Find all transfers involving address A" (from A OR to A)
	// Filter 1: [transfer, addressA, *] (transfers FROM A)
	// Filter 2: [transfer, *, addressA] (transfers TO A)
	// Should return 2 events (A->B and B->A)
	filterTopics := AllFilterTopics{
		// Filter 1: transfers FROM A
		FilterTopics{
			TopicFilter{Positions: [4][]byte{transferBytes, addressABytes, nil, nil}},
		},
		// Filter 2: transfers TO A
		FilterTopics{
			TopicFilter{Positions: [4][]byte{transferBytes, nil, addressABytes, nil}},
		},
	}

	err = eventReader.GetEvents(ctx, cursorRange, nil, filterTopics, nil, EventOrderAsc, scanFunc)
	require.NoError(t, err)
	require.Len(t, foundEvents, 2, "Expected 2 events matching 'transfers involving address A'")

	// Verify: Query with no filters should return more events than the filtered query
	// (confirms the filter is actually filtering, not just returning everything)
	foundEvents = nil
	err = eventReader.GetEvents(ctx, cursorRange, nil, nil, nil, EventOrderAsc, scanFunc)
	require.NoError(t, err)
	require.Greater(t, len(foundEvents), 2, "Expected more events when no filter is applied than when filtered")
}

// TestGetEventsMultipleTopicFiltersWithinFilter verifies OR logic between
// TopicFilters within a single EventFilter.
func TestGetEventsMultipleTopicFiltersWithinFilter(t *testing.T) {
	db := NewTestDB(t)
	ctx := context.TODO()
	logger := log.DefaultLogger
	logger.SetLevel(logrus.TraceLevel)
	now := time.Now().UTC()

	writer := NewReadWriter(logger, db, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(t, err)

	// Create topic values
	transfer := xdr.ScSymbol("transfer")
	mint := xdr.ScSymbol("mint")
	addressA := xdr.ScSymbol("addressA")

	transferTopic := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &transfer}
	mintTopic := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &mint}
	addressATopic := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &addressA}

	contractID := xdr.ContractId([32]byte{1, 2, 3})
	dataVal := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &transfer}

	// Event 1: [transfer, addressA]
	// Event 2: [mint, addressA]
	// Event 3: [burn, addressA] - different action, shouldn't match transfer OR mint

	burn := xdr.ScSymbol("burn")
	burnTopic := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &burn}

	txMeta := []xdr.TransactionMeta{
		transactionMetaWithEvents(contractEvent(contractID, []xdr.ScVal{transferTopic, addressATopic}, dataVal)),
		transactionMetaWithEvents(contractEvent(contractID, []xdr.ScVal{mintTopic, addressATopic}, dataVal)),
		transactionMetaWithEvents(contractEvent(contractID, []xdr.ScVal{burnTopic, addressATopic}, dataVal)),
	}

	ledgerCloseMeta := ledgerCloseMetaWithEvents(1, now.Unix(), txMeta...)

	eventW := write.EventWriter()
	err = eventW.InsertEvents(ledgerCloseMeta)
	require.NoError(t, err)
	require.NoError(t, write.Commit(ledgerCloseMeta, nil))

	eventReader := NewEventReader(logger, db, passphrase)
	cursorRange := protocol.CursorRange{
		Start: protocol.Cursor{Ledger: 1},
		End:   protocol.Cursor{Ledger: 100},
	}

	transferBytes, err := transferTopic.MarshalBinary()
	require.NoError(t, err)
	mintBytes, err := mintTopic.MarshalBinary()
	require.NoError(t, err)
	addressABytes, err := addressATopic.MarshalBinary()
	require.NoError(t, err)

	var foundEvents []protocol.Cursor
	scanFunc := func(event xdr.DiagnosticEvent, cursor protocol.Cursor, ledgerCloseTimestamp int64, txHash *xdr.Hash) bool {
		foundEvents = append(foundEvents, cursor)
		return true
	}

	// Query: Find events where topic1 is transfer OR mint, AND topic2 is addressA
	// This uses multiple TopicFilters within a single Filter (OR between patterns)
	filterTopics := AllFilterTopics{
		FilterTopics{
			TopicFilter{Positions: [4][]byte{transferBytes, addressABytes, nil, nil}}, // transfer + addressA
			TopicFilter{Positions: [4][]byte{mintBytes, addressABytes, nil, nil}},     // mint + addressA
		},
	}

	err = eventReader.GetEvents(ctx, cursorRange, nil, filterTopics, nil, EventOrderAsc, scanFunc)
	require.NoError(t, err)
	require.Len(t, foundEvents, 2, "Expected 2 events matching (transfer OR mint) with addressA")
}
