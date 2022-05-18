package pool

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type callback func() (interface{}, error)
type closeCallback func(connection interface{})

type Connector struct {
	connCb           callback
	closeCb          closeCallback
	connections      chan *Connection
	minActive        uint8
	maxActive        uint16
	maxIdle          time.Duration
	mutex            sync.Mutex
	connectionsCount int32
	retries          uint8
}

type Connection struct {
	activeTime time.Time
	connection *interface{}
}

func GetConnector(connCb callback, closeCb closeCallback, minActive uint8, maxActive uint16, maxIdleSeconds uint16) *Connector {
	connector := &Connector{
		connCb:           connCb,
		closeCb:          closeCb,
		connections:      make(chan *Connection, maxActive),
		minActive:        minActive,
		maxActive:        maxActive,
		mutex:            sync.Mutex{},
		connectionsCount: 0,
		maxIdle:          time.Duration(time.Second * time.Duration(maxIdleSeconds)),
		retries:          3,
	}
	go connector.balanceConnections()
	return connector
}

func (connector *Connector) balanceConnections() {
	for {
		connector.mutex.Lock()
		idleConnectionsCount := len(connector.connections)
		connectionsToCreate := int(connector.minActive) - idleConnectionsCount
		connectionsCount := atomic.LoadInt32(&connector.connectionsCount)
		for ; connectionsToCreate > 0 && connectionsCount < int32(connector.maxActive); connectionsToCreate-- {
			connection, error := connector.connCb()
			if error != nil {
				fmt.Print(error)
				continue
			}
			atomic.AddInt32(&connector.connectionsCount, 1)
			connector.connections <- &Connection{
				connection: &connection,
				activeTime: time.Now(),
			}
		}
		connector.mutex.Unlock()
		time.Sleep(time.Millisecond * 500)
	}
}

func (connector *Connector) Execute(exec func(connection interface{}) (interface{}, error)) (interface{}, error) {
	var lastError error
	for i := connector.retries; i > 0; i-- {
		result, error := connector.execute(exec)
		if error == nil {
			return result, error
		}
		lastError = error
	}
	return nil, lastError
}

func (connector *Connector) execute(exec func(connection interface{}) (interface{}, error)) (interface{}, error) {
	select {
	case connection := <-connector.connections:
		if connection.activeTime.Add(connector.maxIdle).Before(time.Now()) {
			connector.discard(connection)
			return connector.execute(exec)
		} else {
			connection.activeTime = time.Now()
		}
		result, error := connector.exec(exec, connection)
		connector.connections <- connection
		return result, error
	default:
		if int(atomic.LoadInt32(&connector.connectionsCount)) >= int(connector.maxActive) {
			return nil, fmt.Errorf("max active connections %d reached", connector.maxActive)
		}
		connector.mutex.Lock()
		defer connector.mutex.Unlock()
		connectionInstance, error := connector.connCb()
		if error != nil {
			return nil, error
		}
		atomic.AddInt32(&connector.connectionsCount, 1)
		connection := &Connection{connection: &connectionInstance, activeTime: time.Now()}
		result, error := connector.exec(exec, connection)
		connector.connections <- connection
		return result, error
	}
}

func (connector *Connector) exec(exec func(connection interface{}) (interface{}, error), connection *Connection) (interface{}, error) {
	result, error := exec(*connection.connection)
	if error != nil {
		fmt.Print(error)
		connector.discard(connection)
		return nil, error
	}
	return result, nil
}

func (connector *Connector) discard(connection *Connection) {
	atomic.AddInt32(&connector.connectionsCount, -1)
	if connector.closeCb == nil {
		return
	}
	connector.closeCb(*connection.connection)
}
